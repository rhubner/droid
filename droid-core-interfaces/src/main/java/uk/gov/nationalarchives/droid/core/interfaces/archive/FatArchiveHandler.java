package uk.gov.nationalarchives.droid.core.interfaces.archive;


import de.waldheinz.fs.*;
import de.waldheinz.fs.util.FileDisk;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import uk.gov.nationalarchives.droid.core.interfaces.*;
import uk.gov.nationalarchives.droid.core.interfaces.resource.FatFileIdentificationRequest;
import uk.gov.nationalarchives.droid.core.interfaces.resource.FileSystemIdentificationRequest;
import uk.gov.nationalarchives.droid.core.interfaces.resource.RequestMetaData;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FatArchiveHandler implements ArchiveHandler {


    private AsynchDroid droid;

    private ResultHandler resultHandler;

    private final Log log = LogFactory.getLog(this.getClass());

    @Override
    public void handle(IdentificationRequest request) throws IOException {
        if (request.getClass().isAssignableFrom(FileSystemIdentificationRequest.class)) {

            FileSystemIdentificationRequest req = (FileSystemIdentificationRequest) request;


            FileDisk fileDisk = new FileDisk(req.getFile().toFile(), true);

            FileSystem fatSystem = FileSystemFactory.create(fileDisk, true);

            FsDirectory root = fatSystem.getRoot();

            FatArchiveWalker walker = new FatArchiveWalker(droid, resultHandler, fatSystem, request.getIdentifier());
            walker.walk(root);

        } else {
            log.info("Identification request for ISO image ignored due to limited support.");
        }
    }



    private class FatArchiveWalker extends ArchiveFileWalker<FsDirectoryEntry> {


        private final AsynchDroid droid;
        private final ResultHandler resultHandler;


        private final FileSystem fileSystem;
        private final ResourceId rootParentId;
        private final URI fatFileUri;
        private final long originatorNodeId;


        /**
         * Create instance.
         * @param droid async droid.
         * @param resultHandler result handler(used for directory handling).
         * @param fileSystem Original iso file system.
         * @param requestIdentifier ReqIdentifier.
         */
        public FatArchiveWalker(AsynchDroid droid,    ResultHandler resultHandler,
                                FileSystem fileSystem, RequestIdentifier requestIdentifier) {

            this.droid = droid;
            this.resultHandler = resultHandler;
            this.fileSystem = fileSystem;
            this.rootParentId = requestIdentifier.getResourceId();
            this.fatFileUri = requestIdentifier.getUri();
            this.originatorNodeId = requestIdentifier.getNodeId();
            directories.put("", rootParentId);  //Rood directory
        }


        private final Map<String, ResourceId> directories = new HashMap<String, ResourceId>();
        private final Log log = LogFactory.getLog(this.getClass());



        private void submitFile(FsFile file, FsDirectoryEntry entry) throws IOException, URISyntaxException {
            ByteBuffer buffer = ByteBuffer.allocate((int) file.getLength());
            file.read(0, buffer);
            buffer.flip();

            /*
            RequestIdentifier identifier = new RequestIdentifier(ArchiveFileUtils.toIsoImageUri(isoFileUri, path + name));
                identifier.setAncestorId(originatorNodeId);
                identifier.setParentResourceId(correlationId);

                RequestMetaData metaData = new RequestMetaData(entry.getSize(),
                        entry.getLastModifiedTime(), name);

                IdentificationRequest<InputStream> request = factory.newRequest(metaData, identifier);
                request.open(entryInputStream);

                droid.submit(request);
             */

            RequestIdentifier identifier = new RequestIdentifier(new URI("fat://" + entry.getName()));
            identifier.setAncestorId(originatorNodeId);
            identifier.setParentResourceId(rootParentId);

            RequestMetaData requestMetaData = new RequestMetaData(file.getLength(), entry.getCreated(), entry.getName());


            IdentificationRequest<ByteBuffer> req = new FatFileIdentificationRequest(requestMetaData, identifier);
            req.open(buffer);

            droid.submit(req);


        }



        @Override
        protected void handleEntry(FsDirectoryEntry directoryEntry) throws IOException {
            if(directoryEntry.isFile()) {
                try {
                    if(directoryEntry.getFile().getLength() != 0) {
                        submitFile(directoryEntry.getFile(), directoryEntry);
                    }
                } catch (URISyntaxException e) {
                    log.error("url exception : ", e);
                }
            }else if(directoryEntry.isDirectory()) {
                log.info("ignoring dir : " + directoryEntry.getName());
            }else {
                log.error("unknown entry : " + directoryEntry);
            }
        }
    }



    public void aaa() throws IOException {

        FileDisk fileDisk = new FileDisk(new File("/home/rhubner/workspace/droid/Data-080_04-fat12.001"), true);

        FileSystem fatSystem = FileSystemFactory.create(fileDisk, true);

        FsDirectory root = fatSystem.getRoot();

        printDir(root,0);


    }

    public void printDir(FsDirectory root, int ident) throws IOException {
        if(ident > 5) {
            return;
        }
        for (FsDirectoryEntry entry: root) {
            if(entry.isDirectory()) {
                print("directory : " + entry.getName(), ident);

                if(!".".equals(entry.getName()) && !"..".equals(entry.getName())) {
                    printDir(entry.getDirectory(), ident + 2);
                }
            }else if(entry.isFile()) {
                FsFile file = entry.getFile();
                print("file : " + entry.getName() + "\t" + Long.toString(file.getLength()), ident);

            }else {
                print("other : " + entry.getName(), ident);
            }
        }
    }

    public void print(String str, int ident) {
        for(int i = 0 ; i < ident ; i++) {
            System.out.print(" ");
        }
        System.out.println(str);
    }


    public void setDroid(AsynchDroid droid) {
        this.droid = droid;
    }

    public void setResultHandler(ResultHandler resultHandler) {
        this.resultHandler = resultHandler;
    }
}
