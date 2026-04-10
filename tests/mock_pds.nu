export def start-mock-pds [port: int] {
    # kill any stale process from a previous failed run holding this port
    try { bash -c $"fuser -k ($port)/tcp" } catch {}
    sleep 100ms
    let log_file = (mktemp)
    let pid = (bash -c $"websocat -s ($port) >($log_file) 2>&1 & echo $!" | str trim | into int)
    { pid: $pid, log: $log_file }
}

export def stop-mock-pds [handle: record] {
    try { kill $handle.pid }
}
