package compar.bench

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.fs.permission.FsPermission
import java.io.FileNotFoundException

/**
 * Benchmark-only local file system for Windows without winutils.exe: skips the permission and owner
 * calls and lists directories with java.io, the parts of Hadoop that need the native Windows library.
 * Enabled by scripts/local_bench/run1.sh on Windows with -Dspark.hadoop.fs.file.impl=compar.bench.NoNativeLocalFs;
 * nothing else in the project uses it.
 */
class NoNativeRawFs : RawLocalFileSystem() {
    override fun setPermission(p: Path, permission: FsPermission) {}
    override fun setOwner(p: Path, username: String?, groupname: String?) {}

    override fun listStatus(f: Path): Array<FileStatus> {
        val file = pathToFile(f)
        if (!file.exists()) throw FileNotFoundException("File $f does not exist")
        if (file.isFile) return arrayOf(getFileStatus(f))
        return (file.list() ?: emptyArray()).map { getFileStatus(Path(f, it)) }.toTypedArray()
    }
}

class NoNativeLocalFs : LocalFileSystem(NoNativeRawFs())
