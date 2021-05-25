function newFileStorages({ stashPath, size, mtime, executable }) {
    if (size == 0) {
        return {};
    }

    let remoteStorageThreshold;
    if (stashPath.length && stashPath[0] == "Stuff") {
        remoteStorageThreshold = 204800;
    } else {
        remoteStorageThreshold = 60000;
    }

    let lastSegment = stashPath[stashPath.length - 1];
    if (size > remoteStorageThreshold || lastSegment.endsWith(".jpg")) {
        // 1 is the google_domain
        return {gdrive: [1]};
    } else {
        return {inline: true};
    }
}
