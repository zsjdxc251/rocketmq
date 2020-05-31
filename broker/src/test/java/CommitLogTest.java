import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

/**
 * @author zhengshijun
 * @version created on 2020/5/30.
 */
public class CommitLogTest {


    public void test1() throws IOException {

        RandomAccessFile file = new RandomAccessFile( "D:\\workspace\\home\\store\\commitlog\\00000000000000000000", "rw" );

        FileChannel channel = file.getChannel();

        System.out.println(channel.size());
        MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE,0,channel.size());
//
//        for (long i=0;i<channel.size();i++){
//            mappedByteBuffer.put((byte)(i*10));
//        }
//



       // mbb.put(a.getBytes());

        //mbb.flip();
        channel.close();
        byte[] bb = new byte[2014];
        while (mbb.hasRemaining()){
            byte b = mbb.get();
            if (mbb.position() >= 2014){
                break;
            }
            System.out.println(mbb.position());
            bb[mbb.position()]=b;
        }
        System.out.println(new String(bb));

        file.close();


//        byte[] bytes = Files.readAllBytes(Paths.get("D:\\workspace\\home\\store\\commitlog\\00000000000000000000"));
//
//        System.out.println(new String(bytes));

    }

    @Test
    public void test2() throws IOException {

        String fileName_or_phyficalOffset = "00000000000000000000";
        Path path = Paths.get("D:\\workspace\\home\\store\\commitlog", fileName_or_phyficalOffset);
        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        // 设大一点，尽量一次读取完一条消息的完整字节。
        ByteBuffer byteBuffer = ByteBuffer.allocate(2048);

        fileChannel.read(byteBuffer);
        // 输出commitLog的信息


        byteBuffer.flip();
        // 记录消息总长度，每一次操作记录一次，最终与读取的消息总长度对比
        int totalLength = 0;
        int totalLengthF = byteBuffer.getInt();

        System.out.println(totalLengthF);
        //println("TOTALSIZE，该消息总长度4字节",totalLengthF);
        //totalLength+=4;


        System.out.println(byteBuffer.getInt());

        byte[] magic = new byte[4];
        byteBuffer.get(magic);
        System.out.println(new String(magic));

       // System.out.println(new String(byteBuffer.array()));
//        byte[] magic = new byte[4];
//        byteBuffer.get(magic);
//        println("MAGICCODE，魔数，4字节，固定值0xdaa320a7",HexUtil.encodeHexStr(magic));


    }
}
