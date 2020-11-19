function newFileStorages({ stashPath, size, mtime, executable }) {
    let remoteStorageThreshold;
    if (stashPath.length && stashPath[0] == "Stuff") {
        remoteStorageThreshold = 204800;
    } else {
        remoteStorageThreshold = 60000;
    }

    if (fileSize > remoteStorageThreshold || stashPath.endsWith(".jpg")) {
        // 1 is the google_domain
        return {gdrive: [1]};
    } else {
        return {inline: true};
    }
}
