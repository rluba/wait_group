Wait_Group :: struct {
    verbose: bool;
    #if OS == .MACOS {
        kevent_queue: s32 = -1;
    } else #if OS == .LINUX {
        epoll_handle: s32 = -1;
        signals_by_handle: Table(s32, s32); // Map from handle to signal
    } else {
        #assert(false);
    }

    fd_handlers:     Table(Fd_Data, Fd_Handler, given_compare_function = fd_data_is_equal, given_hash_function = hash_fd_data);
    signal_handlers: Table(s32, Signal_Handler);
    user_handlers:   Table(User_Event_Id, User_Handler);

    Fd_Data :: struct {
        fd:     s32;
        event: Wait_Event_Type;
    }

    fd_data_is_equal :: (a: Fd_Data, b: Fd_Data) -> bool {
        return a.fd == b.fd && a.event == b.event;
    }
    hash_fd_data :: (data: Fd_Data) -> u32 {
        return Hash.get_hash(data.fd) ^ Hash.get_hash(cast(int) data.event);
    }
}

Wait_Event_Type :: enum {
    READ;
    WRITE;
}

User_Event_Id :: #type,isa u64;

Fd_Handler :: struct {
    data: *void;
    callback: #type (group: *Wait_Group, handle: s32, is_eof: bool, event: Wait_Event_Type, data: *void);
}

Signal_Handler :: struct {
    data: *void;
    callback: #type (group: *Wait_Group, signal: s32, data: *void);
}

User_Handler :: struct {
    data: *void;
    callback: #type (group: *Wait_Group, event_id: User_Event_Id, data: *void);
}


init :: (group: *Wait_Group, verbose := false) -> success: bool {
    group.verbose = verbose;
    remember_allocators(*group.fd_handlers);
    remember_allocators(*group.signal_handlers);
    remember_allocators(*group.user_handlers);

    #if OS == .LINUX {
        group.epoll_handle = epoll_create1(EPOLL_CLOEXEC);
        if group.epoll_handle < 0 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create epoll instance: % %", error_code, error_string);
            return false;
        }

        remember_allocators(*group.signals_by_handle);
    } else #if OS == .MACOS {
        // @ToDo: This initialization itself is not thread safe
        // We could do a fancy compare_and_swap loop here like in ensure_synchronization_is_initted
        // But for now we just ensure that our first wait group is initialized before we do threaded stuff
        if next_user_event_id == 0 {
            init(*next_user_event_id_mutex);
            next_user_event_id = 1;
        }

        group.kevent_queue = kqueue();
        if group.kevent_queue == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create event queue: % %", error_code, error_string);
            return false;
        }
    }
    return true;
}

deinit :: (group: *Wait_Group) {
    #if OS == .LINUX {
        close_and_reset_fd(*group.epoll_handle);
        for signal, handle: group.signals_by_handle {
            close(handle);
        }
        deinit(*group.signals_by_handle);
        group.signals_by_handle.entries.data = null;
    } else {
        #assert(OS == .MACOS);
        close_and_reset_fd(*group.kevent_queue);
    }

    deinit(*group.fd_handlers);
    group.fd_handlers.entries.data = null;
    deinit(*group.signal_handlers);
    group.signal_handlers.entries.data = null;
    deinit(*group.user_handlers);
    group.user_handlers.entries.data = null;
}

add_handles :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, handle: s32, is_eof: bool, event: Wait_Event_Type, data: *T), handles: .. s32, event_type := Wait_Event_Type.READ) -> success: bool {
    if group.verbose log("Adding handles % for event type %", handles, event_type);
    #if OS == .LINUX {
        for handles {
            action := Epoll_Ctl.ADD;
            events: u32;
            if #complete event_type == {
                case .READ;  events = EPOLLRDHUP | EPOLLIN;
                case .WRITE; events = EPOLLOUT;
            }
            for fd_handler, fd_data: group.fd_handlers {
                if fd_data.fd == it {
                    assert(fd_data.event != event_type, "Trying to add handle % with event %, but there is already a handler for that!", it, event_type);
                    action = .MOD;
                    if #complete fd_data.event == {
                        case .READ;  events |= EPOLLRDHUP | EPOLLIN;
                        case .WRITE; events |= EPOLLOUT;
                    }
                }
            }

            success := update_epoll_handle(group, it, action, events);
            if !success return false;
        }
    } else {
        events := NewArray(handles.count, Kevent64,, temp);
        for handles {
            events[it_index].ident = cast(u64) it;
            events[it_index].flags = .ADD;
            if #complete event_type == {
                case .READ;  events[it_index].filter = .READ;
                case .WRITE; events[it_index].filter = .WRITE;
            }
        }

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register events with the event queue: % %", error_code, error_string);
            return false;
        }
    }

    handler: Fd_Handler;
    handler.data = data;
    handler.callback = xx callback;
    for handles {
        fd_data: Wait_Group.Fd_Data;
        fd_data.fd = it;
        fd_data.event = event_type;
        assert(!table_find_pointer(*group.fd_handlers, fd_data), "Trying to add handle % with event %, but there is already a handler for that!", it, event_type);
        table_add(*group.fd_handlers, fd_data, handler);
    }
    return true;
}

add_signals :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, signal: s32, data: *T), signals: ..s32) -> success: bool {
    if group.verbose log("Adding signals %", signals);
    #if OS == .LINUX {
        for signals {
            handle, success := create_signal_handle(it);
            if !success {
                // @ToDo: Close and remove handles already created
                return false;
            }

            if group.verbose log("Created signal handle % for signal %", handle, signal);

            table_add(*group.signals_by_handle, handle, it);

            success = update_epoll_handle(group, handle, .ADD, EPOLLIN, description = "signal handle");
            if !success return false;
        }
    } else {
        #assert(OS == .MACOS); // @Incomplete
        events := NewArray(signals.count, Kevent64,, temp);
        for signals {
            events[it_index].ident = cast(u64) it;
            events[it_index].filter = .SIGNAL;
            events[it_index].flags = .ADD;
        }
        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register signal events with the event queue: % %", error_code, error_string);
            return false;
        }
    }

    handler: Signal_Handler;
    handler.data = data;
    handler.callback = xx callback;
    for signals {
        assert(!table_find_pointer(*group.signal_handlers, it), "Trying to add signal %, but there is already a handler for that!", it);
        table_add(*group.signal_handlers, it, handler);
    }

    return true;
}

add_user_event :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, event_id: User_Event_Id, data: *T)) -> success: bool, event_id: User_Event_Id #must {
    event_id: User_Event_Id;

    #if OS == .LINUX {
        handle := eventfd(0, xx EFD.NONBLOCK);
        if handle == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create user event handle: % %", error_code, error_string);
            return false, 0;
        }

        success := update_epoll_handle(group, handle, .ADD, EPOLLIN, description = "user event handle");
        if !success return false, 0;

        event_id = cast(User_Event_Id) handle;
    } else {
        #assert(OS == .MACOS); // @Incomplete
        {
            lock(*next_user_event_id_mutex);
            defer unlock(*next_user_event_id_mutex);
            event_id = next_user_event_id;
            next_user_event_id += 1;
        }

        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].filter = .USER;
        events[0].flags = .ADD;
        events[0].fflags = .FFNOP;
        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register user event % with the event queue: % %", event_id, error_code, error_string);
            return false, 0;
        }
    }

    if group.verbose log("Adding user event %", event_id);
    handler: User_Handler;
    handler.data = data;
    handler.callback = xx callback;
    table_add(*group.user_handlers, event_id, handler);

    return true, event_id;
}

add_timer :: (group: *Wait_Group, data: *$T, callback: #type (group: *Wait_Group, handle: User_Event_Id, data: *T), timeout_ms: s64, recurring: bool) -> success: bool, User_Event_Id {
    event_id: User_Event_Id;
    #if OS == .LINUX {
        handle := timerfd_create(xx clockid_t.MONOTONIC, xx TFD.NONBLOCK);
        if handle == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create timer event handle: % %", error_code, error_string);
            return false, 0;
        }

        timeout_spec: timespec;
        timeout_spec.tv_sec = timeout_ms / 1000;
        timeout_spec.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;

        timer_spec: itimerspec;
        timer_spec.it_value = timeout_spec;
        if recurring {
            timer_spec.it_interval = timeout_spec;
        }

        result := timerfd_settime(handle, 0, *timer_spec, null);
        if (result == -1) {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not set timer for timer event handle: % %", error_code, error_string);
            close(handle);
            return false, 0;
        }

        success := update_epoll_handle(group, handle, .ADD, EPOLLIN, description = "timer event handle");
        if !success return false, 0;

        event_id = cast(User_Event_Id) handle;
    } else #if OS == .MACOS {
        {
            lock(*next_user_event_id_mutex);
            defer unlock(*next_user_event_id_mutex);
            event_id = next_user_event_id;
            next_user_event_id += 1;
        }

        flags := Kevent_Flags.ADD;
        if !recurring flags |= .ONESHOT;

        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].filter = .TIMER;
        events[0].flags = flags;
        events[0].data = timeout_ms;

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register user event % with the event queue: % %", event_id, error_code, error_string);
            return false, 0;
        }
    } else {
        #assert(false); // Incomplete
    }

    if group.verbose log("Adding %ms timer (recurring: %) as user event %", timeout_ms, recurring, event_id);
    handler: User_Handler;
    handler.data = data;
    handler.callback = xx callback;
    table_add(*group.user_handlers, event_id, handler);

    return true, event_id;
}

reset_timer :: (group: *Wait_Group, timer: User_Event_Id, timeout_ms: s64, recurring: bool) -> success: bool {
    if group.verbose log("Resetting timer % for %ms (recurring: %)", timer, timeout_ms, recurring);
    #if OS == .LINUX {
        timeout_spec: timespec;
        timeout_spec.tv_sec = timeout_ms / 1000;
        timeout_spec.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;

        timer_spec: itimerspec;
        timer_spec.it_value = timeout_spec;
        if recurring {
            timer_spec.it_interval = timeout_spec;
        }

        result := timerfd_settime(cast(s32) timer, 0, *timer_spec, null);
        if (result == -1) {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not set timer for timer event handle: % %", error_code, error_string);
            return false;
        }
    } else #if OS == .MACOS {
        flags := Kevent_Flags.ADD;
        if !recurring flags |= .ONESHOT;

        events: [1] Kevent64;
        events[0].ident = timer;
        events[0].filter = .TIMER;
        events[0].flags = flags;
        events[0].data = timeout_ms;

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not register user event % with the event queue: % %", timer, error_code, error_string);
            return false;
        }
    } else {
        #assert(false); // Incomplete
    }
    return true;
}

trigger_user_event :: (group: *Wait_Group, event_id: User_Event_Id) -> success: bool {
    if group.verbose log("Triggering user event %", event_id);
    assert(table_find_pointer(*group.user_handlers, event_id) != null, "event_id % is not a user event", event_id);
    #if OS == .LINUX {
        value: u64 = 1;
        size := size_of(type_of(value));
        result := write(cast(s32) event_id, *value, cast(u64) size);
        if result != size {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not write to user event %: % %", event_id, error_code, error_string);
            return false;
        }
    } else {
        #assert(OS == .MACOS); // @Incomplete

        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].filter = .USER;
        events[0].flags = .ENABLE;
        events[0].fflags = Kevent_Fflags.FFCOPY | .TRIGGER | 0x01;
        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not trigger user event % with the event queue: % %", event_id, error_code, error_string);
            return false;
        }
    }

    return true;
}

remove_handle :: (group: *Wait_Group, handle: s32, event_type := Wait_Event_Type.READ) -> success: bool {
    if group.verbose log("Removing handle % for event type %", handle, event_type);

    fd_data: Wait_Group.Fd_Data;
    fd_data.fd = handle;
    fd_data.event = event_type;
    table_remove(*group.fd_handlers, fd_data);

    #if OS == .LINUX {
        action := Epoll_Ctl.DEL;
        events: u32;
        for fd_handler, fd_data: group.fd_handlers {
            if fd_data.fd == handle {
                assert(fd_data.event != event_type, "Trying to remove handle % with event %, but there is more than one handler for that!", handle, event_type);
                action = .MOD;
                if #complete fd_data.event == {
                    case .READ;  events |= EPOLLRDHUP | EPOLLIN;
                    case .WRITE; events |= EPOLLOUT;
                }
            }
        }

        success := update_epoll_handle(group, handle, action, events);
        if !success return false;
    } else {
        events: [1] Kevent64;
        events[0].ident = cast(u64) handle;
        if #complete event_type == {
            case .READ;  events[0].filter = .READ;
            case .WRITE; events[0].filter = .WRITE;
        }
        events[0].flags = .DELETE;

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not remove handle % from the event queue: % %", handle, error_code, error_string);
            return false;
        }
    }

    return true;
}

// @ToDo: remove_signal

remove_user_event :: (group: *Wait_Group, event_id: User_Event_Id) -> success: bool {
    return remove_user_event_or_timer(group, event_id, is_timer = false);
}

remove_timer :: (group: *Wait_Group, timer_id: User_Event_Id) -> success: bool {
    return remove_user_event_or_timer(group, timer_id, is_timer = true);
}

#if OS == .LINUX {
    update_epoll_handle :: (group: *Wait_Group, handle: s32, action: Epoll_Ctl, events: u32, description := "handle") -> success: bool {
        eev: epoll_event;
        eev.data.fd = handle;
        eev.events = events;

        result := epoll_ctl(group.epoll_handle, action, handle, *eev);
        if result == -1 {
            error_code := System.get_error_value();
            if error_code == EBADF && action == .DEL {
                // handle was probably already closed and therefore auto-removed from epoll.
                return true;
            }

            error_string := System.get_error_string(error_code);
            log_error("Could not perform epoll % on % %: % %", action, description, handle, error_code, error_string);
            return false;
        }

        return true;
    }
}

wait_for_events :: (group: *Wait_Group, timeout_ms := -1) -> success: bool {
    success := true;

    #if OS == .LINUX {
        clear :: (handle: s32, $T: Type, description: string) -> bool {
            while true {
                info: T;
                bytes_read := repeat_if_interrupted(read(handle, *info, size_of(type_of(info))));
                if bytes_read == -1 {
                    error_value := errno();
                    if error_value != EAGAIN && error_value != EWOULDBLOCK {
                        error_string := System.get_error_string(error_value);
                        log_error("Could not read % from handle %: % %", description, handle, error_value, error_string);
                        return false;
                    }
                    break;
                }
            }
            return true;
        }

        poll_timeout_ms := cast(s32) timeout_ms;
        events: [16] epoll_event;

        sigset: sigset_t;
        sigemptyset(*sigset);
        sigaddset(*sigset, SIGCHLD);  // default: ignore
        sigaddset(*sigset, SIGCONT);  // default: continue execution
        sigaddset(*sigset, SIGURG);   // default: ignore
        sigaddset(*sigset, SIGWINCH); // default: ignore

        num_events := repeat_if_interrupted(epoll_wait(group.epoll_handle, events.data, events.count, poll_timeout_ms));
        if num_events == -1 {
            error_value, error_string := System.get_error_value_and_string();
            log_error("Couldn't poll events: % %", error_value, error_string);
            return false;
        }

        if group.verbose log("Wait: % events", num_events);
        for i: 0..num_events-1 {
            it := events[i];
            handle := it.data.fd;
            if group.verbose log("Event: %, %", handle, it.events);

            if it.events & EPOLLNVAL {
                log_error("Handle % reported invalid value: %", handle, it.events);
                success = false;
            }

            is_eof := (it.events & EPOLLHUP) != 0;
            if it.events & EPOLLIN || is_eof {
                signal := table_find_pointer(*group.signals_by_handle, handle);
                if signal {
                    // Clear the signal event by reading the signal info
                    if !clear(handle, signalfd_siginfo, "signal info") {
                        success = false;
                    }

                    handler := table_find_pointer(*group.signal_handlers, <<signal);
                    if handler handler.callback(group, <<signal, handler.data);
                    continue;
                }

                {
                    event_id := cast(User_Event_Id) handle;
                    handler := table_find_pointer(*group.user_handlers, event_id);
                    if handler {
                        // Clear the user/timer event by reading the counters
                        if !clear(handle, u64, "event/timer") {
                            success = false;
                        }

                        handler.callback(group, event_id, handler.data);
                        continue;
                    }
                }

                {
                    fd_data: Wait_Group.Fd_Data;
                    fd_data.fd = handle;
                    fd_data.event = .READ;
                    handler := table_find_pointer(*group.fd_handlers, fd_data);
                    if handler handler.callback(group, handle, is_eof, .READ, handler.data);
                    // Don’t stop here, in case POLLOUT is also set!
                }
            }

            if it.events & EPOLLOUT || is_eof {
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .WRITE;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler handler.callback(group, handle, is_eof, .WRITE, handler.data);
            }
        }
    } else {
        #assert(OS == .MACOS);

        timeout_spec_pointer: *timespec = null;
        timeout_spec: timespec;
        if timeout_ms >= 0 {
            timeout_spec.tv_sec = timeout_ms / 1000;
            timeout_spec.tv_nsec = (timeout_ms % 1000) * 1000 * 1000;
            timeout_spec_pointer = *timeout_spec;
        }

        // @ToDo: Max events?
        events: [64] Kevent64;
        num_events := kevent64(group.kevent_queue, null, 0, events.data, events.count, 0, timeout_spec_pointer);
        if num_events == -1 {
            error_code := System.get_error_value();
            if error_code == EINTR {
                // Got interrupted, try again.
                return true;
            }

            error_string := System.get_error_string(error_code);
            log_error("Could not read events from the event queue: % %\n", error_code, error_string);
            return false;
        }

        for 0..num_events-1 {
            event := events[it];
            if event.filter == .SIGNAL {
                signal := cast(s32) event.ident;
                handler := table_find_pointer(*group.signal_handlers, signal);
                if handler handler.callback(group, signal, handler.data);
            } else if event.filter == .READ {
                handle := cast(s32) event.ident;
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .READ;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler {
                    is_eof := (event.flags & .EOF) != 0;
                    handler.callback(group, handle, is_eof, .READ, handler.data);
                }
            } else if event.filter == .WRITE {
                handle := cast(s32) event.ident;
                fd_data: Wait_Group.Fd_Data;
                fd_data.fd = handle;
                fd_data.event = .WRITE;
                handler := table_find_pointer(*group.fd_handlers, fd_data);
                if handler {
                    is_eof := (event.flags & .EOF) != 0;
                    handler.callback(group, handle, is_eof, .WRITE, handler.data);
                }
            } else if event.filter == .USER || event.filter == .TIMER {
                event_id := cast(User_Event_Id) event.ident;
                handler := table_find_pointer(*group.user_handlers, event_id);
                if handler handler.callback(group, event_id, handler.data);
            }
        }
    }

    return success;
}

#scope_module

close_and_reset_fd :: (fd: *s32, loc := #caller_location) {
    if <<fd != -1 {
        result := close(<<fd);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("%: %,%: Could not close file descriptor %: % %", loc.fully_pathed_filename, loc.line_number, loc.character_number, fd, error_code, error_string);
        }
        <<fd = -1;
    }
}

#scope_file

repeat_if_interrupted :: (call: Code) -> s64 #expand {
    while true {
        result := #insert call;
        if result < 0 {
            error_code := errno();
            if error_code == EINTR continue; // Got interrupted, try again
        }

        return result;
    }
}


remove_user_event_or_timer :: (group: *Wait_Group, event_id: User_Event_Id, $is_timer: bool) -> success: bool {
    #if is_timer {
        NAME :: "timer";
    } else {
        NAME :: "user event";
    }
    if group.verbose log("Removing % %", NAME, event_id);

    found := table_remove(*group.user_handlers, event_id);
    assert(found, "Could not find % %", NAME, event_id);

    #if OS == .LINUX {
        handle := cast(s32) event_id;
        result := close(handle);
        if result == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not close % %: % %", NAME, event_id, error_code, error_string);
            return false;
        }
    } else {
        events: [1] Kevent64;
        events[0].ident = event_id;
        events[0].flags = .DELETE;
        #if is_timer {
            events[0].filter = .TIMER;
        } else {
            events[0].filter = .USER;
        }

        result := kevent64(group.kevent_queue, events.data, cast (s32) events.count, null, 0, 0, null);
        if result == -1 {
            error_code := System.get_error_value();
            #if is_timer {
                if error_code == ENOENT {
                    // ONESHOT timers are automatically removed, so if the timer has already fired,
                    // we will get this error if we then try to remove it.
                    return true;
                }
            }
            error_string := System.get_error_string(error_code);
            log_error("Could not remove % % from the event queue: % %", NAME, event_id, error_code, error_string);
            return false;
        }
    }

    return true;
}

#if OS == .MACOS {
    next_user_event_id_mutex: Mutex;
    next_user_event_id: User_Event_Id;
}

#if OS == .LINUX {
    create_signal_handle :: (signal: s32) -> handle: s32, success: bool {
        sig_mask: sigset_t;
        sigemptyset(*sig_mask);
        sigaddset(*sig_mask, signal);

        handle := signalfd(-1, *sig_mask, xx SFD.NONBLOCK);
        if handle == -1 {
            error_code, error_string := System.get_error_value_and_string();
            log_error("Could not create signal handle for signal %: % %", signal, error_code, error_string);
            return -1, false;
        }

        return handle, true;
    }

    #import "Linux";
} else #if OS == .MACOS {
    #import "macos";
}

#import "Basic";
#import "Hash_Table";
#import "POSIX";
#import "Thread";
System :: #import "System";
Hash  :: #import "Hash";


