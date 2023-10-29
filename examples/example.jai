#import,file "../module.jai";

Stuff :: struct {
    data: [] int;
    byte_offset: int;
}

should_quit: bool;
DATA_SIZE :: 10240;
write_stuff: Stuff;
read_stuff:  Stuff;

main :: () {
    wait_group: Wait_Group;
    success := init(*wait_group);
    if !success  exit(1);

    // Create a new socketpair to forward accepts from this socket to the cluster instance:
    sockets: [2] s32;
    result := socketpair(AF_UNIX, .STREAM, 0, *sockets);
    if result != 0 {
        error_code, error_string := get_error_value_and_string();
        log_error("Could not create IPC socket pair: % %", error_code, error_string);
        exit(1);
    }


    #if OS == .MACOS {
        set: s32 = 1;
        result = setsockopt(sockets[0], SOL_SOCKET, SO_NOSIGPIPE, *set, size_of(type_of(set)));
        if result == -1 {
            error_value, error_string := get_error_value_and_string();
            log_error("Could not set socket options: error % %", error_value, error_string);
            exit(1);
        }
        result = setsockopt(sockets[1], SOL_SOCKET, SO_NOSIGPIPE, *set, size_of(type_of(set)));
        if result == -1 {
            error_value, error_string := get_error_value_and_string();
            log_error("Could not set socket options: error % %", error_value, error_string);
            exit(1);
        }
    }

    success = set_blocking(sockets[0], false);
    if !success exit(1);
    success = set_blocking(sockets[1], false);
    if !success exit(1);

    write_stuff.data = NewArray(DATA_SIZE, int);
    for i: 0..DATA_SIZE-1 {
        write_stuff.data[i] = i;
    }

    read_stuff.data = NewArray(DATA_SIZE, int);

    success = add_signals(*wait_group, null, handle_signal_event, SIGINT, SIGTERM);
    if !success exit(1);

    success = add_handles(*wait_group, *read_stuff, handle_ready_to_read, sockets[0]);
    if !success exit(1);

    success = add_handles(*wait_group, *write_stuff, handle_ready_to_write, sockets[1], event_type = .WRITE);
    if !success exit(1);

    TIMEOUT_MS :: 3_000;
    success = add_timer(*wait_group, null, on_timeout, TIMEOUT_MS, recurring = false);
    if !success exit(1);

    while !should_quit {
        success := wait_for_events(*wait_group);
        if !success {
            error_code, error_string := get_error_value_and_string();
            log_error("Couldn’t wait for events. Exiting…");
            exit(1);
        }
    }
}

handle_ready_to_write :: (group: *Wait_Group, handle: s32, is_eof: bool, event: Wait_Event_Type, stuff: *Stuff) {
    log("Got ready to write signal for % (eof: %)", handle, is_eof);
    assert(event == .WRITE);
    data := cast(*u8) stuff.data.data + stuff.byte_offset;
    remaining := stuff.data.count * size_of(int) - stuff.byte_offset;
    while remaining {
        flags: MSG;
        #if OS == .LINUX {
            flags |= .NOSIGNAL;
        }
        bytes_written := send(handle, data, cast(u64) remaining, flags);
        if bytes_written == -1 {
            error_code := get_last_socket_error();
            if error_code == EAGAIN || error_code == EWOULDBLOCK {
                log("Write buffer is full after % bytes, waiting until more space becomes available.", write_stuff.byte_offset);
                return; // Wait until we’re called again, when there’s more room.
            }

            error_string := get_error_string(error_code);
            log_error("Could not send data: % %", error_code, error_string);
            exit(1);
        }

        assert(bytes_written <= remaining);
        data += bytes_written;
        remaining -= bytes_written;
        stuff.byte_offset += bytes_written;

        if !remaining {
            log("Finished writing all data. Closing socket.");
            close(handle);
        }
    }
}

handle_ready_to_read :: (group: *Wait_Group, handle: s32, is_eof: bool, event: Wait_Event_Type, stuff: *Stuff) {
    log("Got ready to read signal for % (eof: %)", handle, is_eof);
    assert(event == .READ);
    data := cast(*u8) stuff.data.data + stuff.byte_offset;
    remaining := stuff.data.count * size_of(int) - stuff.byte_offset;
    while remaining {
        flags: MSG;
        #if OS == .LINUX {
            flags |= .NOSIGNAL;
        }
        bytes_read := recv(handle, data, cast(u64) remaining, flags);
        if bytes_read < 0 {
            read_error := errno();
            if read_error == EAGAIN || read_error == EWOULDBLOCK {
                return; // Wait until we’re called again, when there’s more data.
            }

            error_string := get_error_string(read_error);
            log_error("Could not receive data: % %", read_error, error_string);
            exit(1);
        }

        log("Read % bytes", bytes_read);

        assert(bytes_read <= remaining);
        data += bytes_read;
        remaining -= bytes_read;
        stuff.byte_offset += bytes_read;
        if !remaining {
            log("Finished reading all data. Waiting for EOF…");
            for stuff.data {
                assert(it == it_index, "Read unexpected data at index %: %", it_index, it);
            }
        }
    }

    if is_eof {
        assert(!remaining);
        log("Received EOF. Qutting.");
        should_quit = true;
    }
}

on_timeout :: (group: *Wait_Group, handle: User_Event_Id, data: *void) {
    log_error("Timeout reached!");
    should_quit = true;
}

handle_signal_event :: (group: *Wait_Group, signal: s32, data: *void) {
    if signal == {
        case SIGINT; #through;
        case SIGTERM;
            log("Caught signal %. Shutting down…", signal);
            should_quit = true;
        case;
            log("Caught unexpected signal: %", signal);
            exit(1);
    }
}

#import "Basic";
#import "POSIX";
#import "Socket";
#import "System";
