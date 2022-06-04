function new_file_storages({ stash_path, size, mtime, executable }) {
    if (size == 0) {
        return {};
    }

    let remote_storage_threshold = 60000;
    if (stash_path.length && stash_path[0] == "Stuff") {
        remote_storage_threshold = 204800;
    }

    const last_segment = stash_path[stash_path.length - 1];
    if (size > remote_storage_threshold || last_segment.endsWith(".jpg")) {
        // 1 is the google_domain
        return {gdrive: [1]};
    } else {
        return {inline: true};
    }
}
