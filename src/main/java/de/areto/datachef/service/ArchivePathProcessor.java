package de.areto.datachef.service;

public class ArchivePathProcessor {

     /*private void handleArchive() throws IOException {
        final URI zipUri = URI.create("jar:file:" + path.toAbsolutePath().toString());

        try (FileSystem fs = FileSystems.newFileSystem(zipUri, Collections.emptyMap())) {
            final List<Path> paths = Files.walk(fs.getPath("/")).sorted().collect(Collectors.toList());
            for(Path zipPath : paths) {
                if(ignorable(zipPath))
                    continue;

                final String tmpDir = System.getProperty("java.io.tmpdir");
                final Path target = Paths.get(tmpDir, zipPath.getFileName().toString());
                Files.copy(zipPath, target);
            }
        }
    }*/

}
