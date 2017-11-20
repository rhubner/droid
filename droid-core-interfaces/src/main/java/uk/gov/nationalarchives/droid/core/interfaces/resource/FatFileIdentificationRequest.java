package uk.gov.nationalarchives.droid.core.interfaces.resource;

import net.byteseek.io.reader.ByteArrayReader;
import net.byteseek.io.reader.WindowReader;
import org.apache.commons.lang.NotImplementedException;
import uk.gov.nationalarchives.droid.core.interfaces.IdentificationRequest;
import uk.gov.nationalarchives.droid.core.interfaces.RequestIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class FatFileIdentificationRequest implements IdentificationRequest<ByteBuffer> {


    private final String fileName;
    private final String extension;

    private final RequestMetaData requestMetaData;
    private final RequestIdentifier identifier;

    private long size;

    private WindowReader reader;

    public FatFileIdentificationRequest(RequestMetaData requestMetaData, RequestIdentifier identifier) {
        this.requestMetaData = requestMetaData;
        this.identifier = identifier;
        size = requestMetaData.getSize();
        fileName = requestMetaData.getName();
        extension = ResourceUtils.getExtension(fileName);
    }


    @Override
    public void open(ByteBuffer bytesource) throws IOException {
        reader = new ByteArrayReader(bytesource.array());
    }


    @Override
    public byte getByte(long position) throws IOException {
        return (byte) reader.readByte(position);
    }

    @Override
    public WindowReader getWindowReader() {
        return reader;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public String getExtension() {
        return extension;
    }

    @Override
    public InputStream getSourceInputStream() throws IOException {
        throw new NotImplementedException("not implemented");
    }

    @Override
    public RequestMetaData getRequestMetaData() {
        return requestMetaData;
    }

    @Override
    public RequestIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
