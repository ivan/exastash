// Return an object indicating which storages a new file should be stored into.
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
        // 1 is the google domain id
        // 5 is the fofs pile id
        return {gdrive: [1], fofs: [5]};
    } else {
        return {inline: true};
    }
}

// Return a string, the URL at which a remote (i.e. not on localhost) fofs pile is reachable
function fofs_base_url(pile_hostname) {
    return `http://${pile_hostname}.wg:31415`;
}
