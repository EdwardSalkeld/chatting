package metrics

import (
	"fmt"
	"os"
	"strings"
)

// RenderSQLite reports apparent file sizes without opening or locking SQLite.
func RenderSQLite(path string) string {
	var out strings.Builder
	out.WriteString("# HELP chatting_sqlite_file_size_bytes Apparent SQLite file size in bytes.\n# TYPE chatting_sqlite_file_size_bytes gauge\n")
	out.WriteString("# HELP chatting_sqlite_file_stat_success Whether the SQLite file size could be read (absent optional sidecars count as success).\n# TYPE chatting_sqlite_file_stat_success gauge\n")
	for _, file := range []struct{ name, suffix string }{{"database", ""}, {"wal", "-wal"}, {"shm", "-shm"}} {
		info, err := os.Stat(path + file.suffix)
		success := err == nil || (file.suffix != "" && os.IsNotExist(err))
		if success {
			var size int64
			if info != nil {
				size = info.Size()
			}
			fmt.Fprintf(&out, "chatting_sqlite_file_size_bytes{component=\"handler\",file=%q} %d\n", file.name, size)
		}
		value := 0
		if success {
			value = 1
		}
		fmt.Fprintf(&out, "chatting_sqlite_file_stat_success{component=\"handler\",file=%q} %d\n", file.name, value)
	}
	return out.String()
}
