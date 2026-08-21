import os
import fnmatch
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import xml.etree.ElementTree as ET
import threading

import inotify.adapters
from inotify.constants import (
    IN_MODIFY,
    IN_CREATE,
    IN_DELETE,
    IN_MOVED_FROM,
    IN_MOVED_TO
    )

from ..include.config import (
    get_dir_path, 
    get_retry_interval, 
    get_scan_interval, 

    is_text,

    open_glob_mask,
    source_key_from_obj,

    LogFormat, 
    JournaldFilter, 
    MultilineType, 
    Multiline, 
    LocalFileRaw, 
    LocalFile
    )

@dataclass
class AgentConfig:

    conf_dir_path: Path = get_dir_path()
    conf_retry_interval: float = get_retry_interval()
    conf_scan_interval: float = get_scan_interval()

class AgentLogConfig:

    def __init__(
              self, 
              conf_dir_path: Path = AgentConfig.conf_dir_path,
              conf_retry_interval: float = AgentConfig.conf_retry_interval,
              conf_scan_interval: float = AgentConfig.conf_scan_interval
              ) -> None:
        
        self.conf_dir_path = Path(conf_dir_path)
        self.conf_retry_interval = float(conf_retry_interval)
        self.conf_scan_interval = float(conf_scan_interval)

        self._conf_mtimes: dict[str, float] = {}
        self._conf_mtimes_old: dict[str, float] = {}
        self._valid_confs: dict[tuple, LocalFile] = {}
        self._valid_localfile_confs: dict[tuple, set[tuple]] = {}
        self._unvalid_confs: dict[tuple, LocalFileRaw] = {}

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._threads = list[threading.Thread] = []

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def load_all(self) -> list[LocalFile]:
        with self._lock:
            self._load_conf_dir()
            self._validate_unvalid_confs()
            return list(self._valid_confs.values())

    def get_valid_confs(self) -> list[LocalFile]:
        with self._lock:
            return list(self._valid_confs.values())

    def get_unvalid_confs(self) -> list[LocalFileRaw]:
        with self._lock:
            return list[self._unvalid_confs.values()]

    def start(self) -> None:

        if self._threads:
            return

        self._stop_event.clear()

        thread_watch = threading.Thread(
            target=self._watch_loop, 
            daemon=True, 
            name="logcollector-config-watch"
            )

        thread_retry = threading.Thread(
            target=self._retry_loop, 
            daemon=True, 
            name="logcollector-config-retry"
            )

        thread_watch.start()
        thread_retry.start()
        self._threads.extend([thread_watch, thread_retry])

    def stop(self) -> None:

        self._stop_event.set()

        for t in self._threads:
            t.join(timeout=5.0)

        self._threads.clear()

    # ------------------------------------------------------------------
    # apload .conf files
    # ------------------------------------------------------------------

    def _load_conf_dir(self) -> None:

        current_mtimes: dict[str, float] = {}

        for conf_file in self.conf_dir_path.glob("*.conf"):
            if conf_file.is_file():
                current_mtimes[str(conf_file)] = conf_file.stat().st_mtime

        if current_mtimes == self._conf_mtimes:
            return

        self._conf_mtimes = current_mtimes

        all_localfiles = dict[tuple, LocalFileRaw] = {}

        for conf_file in current_mtimes:
            parsed_cf = self._parse_conf_file(conf_file)

            for localfile in parsed_cf:
                key = localfile.dedup_key()

                if key in all_localfiles:
                    continue

                all_localfiles[key] = localfile

        new_keys = set(all_localfiles)
        old_keys = set(self._conf_mtimes_old)
        removed = old_keys - new_keys
        added = new_keys - old_keys

        for k in removed:
            self._clear_localfile_by_key(k)

        for k in added:
            self._try_validate(all_localfiles[k])

        self._conf_mtimes_old = all_localfiles

    def _clear_localfile_by_key(self, localfile_key: tuple) -> None:

        for source_key in self._valid_localfile_confs.pop(localfile_key, set()):
            self._valid_confs.pop(source_key, None)

        self._unvalid_confs(localfile_key, None)

    def _parse_conf_file(self, conf_path: str) -> list[LocalFileRaw]:

        try:
            tree = ET.parse(conf_path)
        except ET.ParseError as e:
            print(f"parse error: {e}")
            raise

        root = tree.getroot()
        localfiles: list[LocalFileRaw] = []

        for localfile_node in root.findall("localfile"):

            localfile = self._parse_localfile(localfile_node)

            if localfile is not None:
                localfiles.append(localfile)

        return localfiles

    def _parse_localfile(self, localfile_node: ET.Element, conf_location: str) -> Optional[LocalFileRaw]:

        log_format_raw = is_text(localfile_node.find("log_format").text)
        if not log_format_raw:
            return None

        try:
            log_format = LocalFileRaw(log_format_raw)
        except ValueError:
            return None

        location_raw = is_text(localfile_node.find("location").text)
        if not location_raw:
            return None

        journal_filter = self.parse_filter(localfile_node, log_format)

        multiline = self.parse_multiline(localfile_node, log_format)

        return(LocalFileRaw(
            log_format=log_format,
            location=location_raw,
            filter=journal_filter,
            multiline=multiline,
            conf_location=conf_location
        ))

    # ------------------------------------------------------------------
    # source validator
    # ------------------------------------------------------------------

    def _try_validate(self, localfile: LocalFileRaw) -> None:

        localfile_key = localfile.dedup_key()

        if localfile.log_format == LogFormat.JOURNALD:
            self._unvalid_confs.pop(localfile_key, None)
            source = LocalFile(
                log_format=localfile.log_format,
                location=localfile.location,
                filter=localfile.filter,
                multiline=localfile.multiline,
                conf_location=localfile.conf_location
            )
            self._add_source(localfile_key, source)
            return

        opened_mask = open_glob_mask(localfile.location)

        if not opened_mask:
            self._unvalid_confs[localfile_key] = localfile
            self._clear_localfile_source(localfile_key)
            return

        self._clear_localfile_source(localfile_key)
        any_failed = False

        for real_path in opened_mask:

            if not os.path.exists(real_path):
                any_failed = True
                continue

            if not os.access(real_path, os.R_OK):
                any_failed = True
                continue

            source = LocalFile(
                log_format=localfile.log_format,
                location=localfile.location,
                filter=localfile.filter,
                multiline=localfile.multiline,
                conf_location=localfile.conf_location
            )

            self._add_source(localfile_key, source)

            if any_failed:
                self._unvalid_confs(localfile_key) = localfile
            else:
                self._unvalid_confs.pop(localfile_key, None)

    def _add_source(self, localfile_key: tuple, source: LocalFile) -> None:

        key = source_key_from_obj(source)

        existing = self._valid_confs.get(key)

        if existing is not None and existing.conf_location != source.conf_location:
            return

        self._valid_confs[key] = source
        self._valid_localfile_confs.setdefault(localfile_key, set()).add(key)

    def _clear_localfile_source(self, localfile_key: tuple) -> None:

        for source_key in self._valid_localfile_confs.pop(localfile_key, set()):
            self._valid_confs.pop(source_key, None)

    def _validate_unvalid_confs(self) -> None:

        for localfile_key, localfile in list(self._unvalid_confs.items()):
            self._try_validate(localfile)

    # ------------------------------------------------------------------
    # Hot reload
    # ------------------------------------------------------------------

    def _watch_loop(self) -> None:

        watcher: _Watcher
        inotify_watcher = _InotifyWatcher(self.conf_dir_path, "*.conf", self._conf_changing)

        watcher = inotify_watcher

        watcher.start()

        try:

            while not self._stop_event.is_set():
                self._stop_event.wait(self.conf_scan_interval)

                with self._lock:
                    self._load_conf_dir()
                    self._validate_unvalid_confs()
        finally:
            watcher.stop()
        

    def _retry_loop(self) -> None:

        while not self._stop_event.wait(self.conf_scan_interval):
            with self._lock:
                if not self._unvalid_confs:
                    continue

                self._validate_unvalid_confs()

    def _conf_changing(self) -> None:
        with self._lock:
            self._load_conf_dir()
            self._validate_unvalid_confs()

class _Watcher:

    def __init__(
            self, 
            conf_dir_path: Path, 
            pattern: str, 
            callback: callable[[str], None]
            ) -> None:

        self.conf_dir_path = conf_dir_path
        self.pattern = pattern
        self.callback = callback
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:

        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="conf-watcher")
        self._thread.start()

    def stop(self) -> None:

        self._stop.set()

        if self._thread is not None:

            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        raise NotImplementedError

class _InotifyWatcher(_Watcher):

    def start(self) -> None:

        self._mask = IN_MODIFY | IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO
        self._inotify = inotify.adapters.Inotify()

        self._inotify.add_watch(str(self.conf_dir_path))

        super().start()

    def _run(self) -> None:

        for event in self._inotify.event_gen(yield_nones=False, timeout_s=1):

            if self._stop.is_set():
                break

            if event is None:
                continue

            (_, type_names, path, filename) = event
            fname = filename or ""

            if not fnmatch.fnmatch(fname, self.pattern):
                continue

            self.callback(os.path.join(path, fname))

    def stop(self) -> None:

        super().stop()

        try:
            self._inotify.remove_watch(str(self.conf_dir_path))
        except Exception:
            pass