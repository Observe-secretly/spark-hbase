package cn.tsign.common.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import cn.tsign.common.config.ConfigConstant;

public class HdfsUtils implements Serializable {

    private static final long serialVersionUID = 1428691036607245503L;

    private String            uri              = ConfProperties.getStringValue(ConfigConstant.hdfs_uri);

    public static Configuration getConfig() {
        Configuration conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        return conf;
    }

    /**
     * make a new dir in the hdfs
     * 
     * @param dir the dir may like '/tmp/testdir'
     * @return boolean true-success, false-failed
     * @exception IOException something wrong happends when operating files
     */
    public boolean mkdir(String dir) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return false;
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        if (!fs.exists(new Path(dir))) {
            fs.mkdirs(new Path(dir));
        }

        fs.close();
        return true;
    }

    /**
     * delete a dir in the hdfs. if dir not exists, it will throw FileNotFoundException
     * 
     * @param dir the dir may like '/tmp/testdir'
     * @return boolean true-success, false-failed
     * @exception IOException something wrong happends when operating files
     */
    public boolean deleteDir(String dir) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return false;
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        fs.delete(new Path(dir), true);
        fs.close();
        return true;
    }

    /*
     * upload the local file to the hds, notice that the path is full like /tmp/test.txt if local file not exists, it
     * will throw a FileNotFoundException
     * @param localFile local file path, may like F:/test.txt or /usr/local/test.txt
     * @param hdfsFile hdfs file path, may like /tmp/dir
     * @return boolean true-success, false-failed
     * @throws IOException file io exception
     */
    public List<String> listAll(String dir) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        FileStatus[] stats = fs.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isDir()) {
                // dir
                names.add(stats[i].getPath().toString());
            } else {
                // regular file
                names.add(stats[i].getPath().toString());
            }
        }

        fs.close();
        return names;
    }

    public FileStatus[] listFileStatus(String dir) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return null;
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        FileStatus[] stats = fs.listStatus(new Path(dir));
        return stats;
    }

    public List<String> listFile(String dir) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        FileStatus[] stats = fs.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                names.add(stats[i].getPath().toString());
            }
        }
        fs.close();
        return names;
    }

    public List<String> listFileTopN(String dir, int N) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        FileStatus[] stats = fs.listStatus(new Path(dir));

        TreeMap<Long, String> treeMap = new TreeMap<>(new Comparator<Long>() {

            @Override
            public int compare(Long k1, Long k2) {
                return (int) (k2 - k1);
            }
        });

        for (int i = 0; i < stats.length; ++i) {
            FileStatus file = stats[i];
            if (file.isFile()) {
                Random rundom = new Random();
                treeMap.put(file.getModificationTime() + rundom.nextInt(100), file.getPath().toString());
            }
        }
        fs.close();
        List<String> names = new ArrayList<String>();

        for (Entry<Long, String> entry : treeMap.entrySet()) {
            names.add(entry.getValue());
            if (names.size() >= N) {
                break;
            }
        }

        return names;
    }

    public List<String> listDir(String dir) throws IOException {
        if (StringUtil.isBlank(dir)) {
            return new ArrayList<String>();
        }
        dir = formatHdfsPath(dir);
        FileSystem fs = FileSystem.get(URI.create(dir), getConfig());
        FileStatus[] stats = fs.listStatus(new Path(dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (!stats[i].isFile()) {
                // dir
                names.add(stats[i].getPath().toString());
            }
        }
        fs.close();
        return names;
    }

    /*
     * upload the local file to the hds, notice that the path is full like /tmp/test.txt if local file not exists, it
     * will throw a FileNotFoundException
     * @param localFile local file path, may like F:/test.txt or /usr/local/test.txt
     * @param hdfsFile hdfs file path, may like /tmp/dir
     * @return boolean true-success, false-failed
     * @throws IOException file io exception
     */
    public boolean uploadLocalFile2HDFS(String localFile, String hdfsFile) throws IOException {
        if (StringUtil.isBlank(localFile) || StringUtil.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = formatHdfsPath(hdfsFile);
        FileSystem hdfs = FileSystem.get(URI.create(uri), getConfig());
        Path src = new Path(localFile);
        Path dst = new Path(hdfsFile);
        hdfs.copyFromLocalFile(src, dst);
        hdfs.close();
        return true;
    }

    /**
     * create a new file in the hdfs. if dir not exists, it will create one
     * 
     * @param newFile new file path, a full path name, may like '/tmp/test.txt'
     * @param content file content
     * @return boolean true-success, false-failed
     * @throws IOException file io exception
     */
    public boolean createNewHDFSFile(String newFile, String content) throws IOException {
        if (StringUtil.isBlank(newFile) || null == content) {
            return false;
        }
        newFile = formatHdfsPath(newFile);
        FileSystem hdfs = FileSystem.get(URI.create(newFile), getConfig());
        FSDataOutputStream os = hdfs.create(new Path(newFile));
        os.write(content.getBytes("UTF-8"));
        os.close();
        hdfs.close();
        return true;
    }

    /**
     * delete the hdfs file
     * 
     * @param hdfsFile a full path name, may like '/tmp/test.txt'
     * @return boolean true-success, false-failed
     * @throws IOException file io exception
     */
    public boolean deleteHDFSFile(String hdfsFile) throws IOException {
        if (StringUtil.isBlank(hdfsFile)) {
            return false;
        }
        hdfsFile = formatHdfsPath(hdfsFile);
        FileSystem hdfs = FileSystem.get(URI.create(hdfsFile), getConfig());
        Path path = new Path(hdfsFile);
        boolean isDeleted = hdfs.delete(path, true);
        hdfs.close();
        return isDeleted;
    }

    /**
     * read the hdfs file content
     * 
     * @param hdfsFile a full path name, may like '/tmp/test.txt'
     * @return byte[] file content
     * @throws IOException file io exception
     */
    public byte[] readHDFSFile(String hdfsFile) throws Exception {
        if (StringUtil.isBlank(hdfsFile)) {
            return null;
        }
        hdfsFile = formatHdfsPath(hdfsFile);

        FileSystem fs = FileSystem.get(URI.create(hdfsFile), getConfig());
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            is.readFully(0, buffer);
            is.close();
            fs.close();
            return buffer;
        } else {
            throw new Exception("the file is not found .");
        }
    }

    public InputStream readHDFSFileToStream(String hdfsFile) throws Exception {
        if (StringUtil.isBlank(hdfsFile)) {
            return null;
        }
        hdfsFile = formatHdfsPath(hdfsFile);

        FileSystem fs = FileSystem.get(URI.create(hdfsFile), getConfig());
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            FSDataInputStream is = fs.open(path);
            return is;
        } else {
            throw new Exception("the file is not found .path is " + path);
        }
    }

    private String formatHdfsPath(String hdfsPath) {
        if (!hdfsPath.startsWith(uri)) {
            hdfsPath = uri + hdfsPath;
        }
        return hdfsPath;
    }

    /**
     * append something to file dst
     * 
     * @param hdfsFile a full path name, may like '/tmp/test.txt'
     * @param content string
     * @return boolean true-success, false-failed
     * @throws Exception something wrong
     */
    public boolean append(String hdfsFile, String content) throws Exception {
        if (StringUtil.isBlank(hdfsFile)) {
            return false;
        }
        if (StringUtil.isEmpty(content)) {
            return true;
        }

        hdfsFile = formatHdfsPath(hdfsFile);
        Configuration conf = new Configuration();
        // solve the problem when appending at single datanode hadoop env
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
        // check if the file exists
        Path path = new Path(hdfsFile);
        if (fs.exists(path)) {
            try {
                InputStream in = new ByteArrayInputStream(content.getBytes());
                OutputStream out = fs.append(new Path(hdfsFile));
                IOUtils.copyBytes(in, out, 8192, true);
                out.close();
                in.close();
                fs.close();
            } catch (Exception ex) {
                fs.close();
                throw ex;
            }
        } else {
            createNewHDFSFile(hdfsFile, content);
        }
        return true;
    }

    /**
     * 文件是否存在
     * 
     * @param hdfsPath
     * @return
     * @throws Exception
     */
    public boolean exist(String hdfsPath) throws Exception {
        if (StringUtil.isBlank(hdfsPath)) {
            return false;
        }

        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(uri), getConfig());
            Path path = new Path(hdfsPath);
            return fs.exists(path);
        } finally {
            fs.close();
        }

    }

    /**
     * 是否是目录
     * 
     * @param dir
     * @return
     * @throws IOException
     */
    public boolean isDirectory(String dir) throws IOException {

        FileSystem fileSystem = null;

        try {
            fileSystem = FileSystem.get(URI.create(uri), getConfig());
            Path path = new Path(dir);
            return fileSystem.isDirectory(path);
        } finally {
            fileSystem.close();
        }
    }

    /**
     * 重命名
     * 
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    public boolean rename(String src, String dst) throws IOException {
        FileSystem fileSystem = null;

        try {
            fileSystem = FileSystem.get(URI.create(uri), getConfig());
            return fileSystem.rename(new Path(src), new Path(dst));
        } finally {
            fileSystem.close();
        }
    }

    /**
     * 移动文件<br>
     * 牛逼的rename
     * 
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    public boolean move(String src, String dst) throws IOException {
        FileSystem fileSystem = null;

        try {
            fileSystem = FileSystem.get(URI.create(uri), getConfig());
            return fileSystem.rename(new Path(src), new Path(dst));
        } finally {
            fileSystem.close();
        }
    }

}
