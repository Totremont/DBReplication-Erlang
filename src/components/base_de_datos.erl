-module(base_de_datos).

-export([init/0, start/2, say/1, stop/0, stop/1, put/4, put/5, del/3, del/4]).

init() ->
    Pid_msgHandler = spawn(fun() -> msgHandler(maps:new()) end),
    register(msgHandler, Pid_msgHandler),
    io:format("Message handler started with PID: ~p~n", [Pid_msgHandler]).

msgHandler(Names) -> % Names es un map que contiene el nombre de la replica y la cantidad de replicas {Replica, Cantidad}
    receive
        {start, Src, Name, N} ->
            case lists:member(Name, maps:keys(Names)) of
                true ->
                    Src ! {error, "Name already exists."};
                false ->
                    case N of
                        N when N > 0 ->
                            NamesList = create(Name, N, []),
                            Src ! {ok, "Databases created."},
                            NewNames = maps:put(Name, NamesList, Names),
                            msgHandler(NewNames);
                        _ ->
                            Src ! {error, "N must be greater than 0."}
                    end
            end,
            msgHandler(Names);
        {stop, Src, Name} ->
            case maps:find(Name, Names) of
                error ->
                    Src ! {error, "Replic name not found"};
                {ok, Value} ->
                    stopRep(Value),
                    Src ! {ok, ""},
                    NewNames = maps:remove(Name, Names),
                    msgHandler(NewNames)
            end;
        {stop, Src} ->
            AllReplicas = lists:flatten(maps:values(Names)),
            stopRep(AllReplicas),
            Src ! {ok, "All replicas stopped."},
            msgHandler(maps:new());
        {put, Src, Key, Value, Timestamp, one, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInit([Coord], [], Src, one) end),
                            sendPutTo(List, CoordPid, Key, Value, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end;
        {put, Src, Key, Value, Timestamp, quorum, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInit(List, [], Src, quorum) end),
                            sendPutTo(List, CoordPid, Key, Value, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end;
        {put, Src, Key, Value, Timestamp, all, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInit(List, [], Src, all) end),
                            sendPutTo(List, CoordPid, Key, Value, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end;
        {del, Src, Key, Timestamp, one, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInit([Coord], [], Src, all) end),
                            sendDelTo(List, CoordPid, Key, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end;
        {del, Src, Key, Timestamp, quorum, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInit(List, [], Src, quorum) end),
                            sendDelTo(List, CoordPid, Key, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end;
        {del, Src, Key, Timestamp, all, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInit(List, [], Src, all) end),
                            sendDelTo(List, CoordPid, Key, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end
    end.

create(_, 0, List) ->
    io:format("[create/2] Databases created and registered.~n"),
    List;
create(Name, N, List) when N > 0 ->
    ReplicName = list_to_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(N)),
    %Pid_newDb = spawn(fun() -> replica:start(self(), ReplicName) end),
    spawn(fun() -> replica:start(self(), ReplicName) end),
    %register(ReplicName, Pid_newDb),
    %io:format("[create/2] Database [~p] created with PID: ~p.~n", [ReplicName, Pid_newDb]),
    create(Name, N-1, [ReplicName | List]).

start(Name, N) ->
    msgHandler ! {start, self(), Name, N},
    receive
        {error, Reason} ->
            io:format("[start/2] Error al crear la base de datos con nombre ~p.~nReason: ~p", [Name, Reason]);
        {ok, _} ->
            io:format("[start/2] Bases de datos creadas.~n")
    end.

stop(Name) ->
    msgHandler ! {stop, self(), Name},
    receive
        {ok, _} ->
            io:format("[stop/1] Se detuvieron las réplicas. ~n");
        {error, Reason} ->
            io:format("[stop/1] Error al detener las replicas.~n~pReason: ", [Reason])
    end.
stop() ->
    msgHandler ! {stop, self()},
    receive
        {ok, _} ->
            io:format("[stop/0] Se detuvieron todas las réplicas.~n");
        {error, Reason} ->
            io:format("[stop/0] Error al detener todas las réplicas.~nReason: ~p", [Reason])
    end.

put(Key, Value, Consistency, Coord)->
    put(Key, Value, calendar:local_time(), Consistency, Coord).    
put(Key, Value, Timestamp, Consistency, Coord) ->
    msgHandler ! {put, self(), Key, Value, Timestamp, Consistency, Coord},
    receive
        {ok, Reply} ->
            io:format("[put/5] Se insertó el par Clave-Valor.~n[Coords] ~p~n", [Reply]);
        {error, _} ->
            io:format("[put/5] Error al insertar el par Clave-Valor.~n")
    end.

del(Key, Consistency, Coord) ->
    del(Key, calendar:local_time(), Consistency, Coord).
del(Key, Timestamp, Consistency, Coord) ->
    msgHandler ! {del, self(), Key, Timestamp, Consistency, Coord},
    receive
        {ok, Reply} ->
            io:format("[put/5] Se eliminó la clave.~n[Coords] ~p~n", [Reply]);
        {error, _} ->
            io:format("[put/5] Error al eliminar la clave.~n")
    end.

say(Replica) -> % Es para probar que las replicas se encuentren activas, say(replica, mensaje) donde replica es un LIST! importante!
    Replica ! {say, self()},
    receive
        {ok, Reply}->
            io:format("[say/1] [~p] Map: ~p~n", [Replica, Reply]);
        _ ->
            ok
    end.

stopRep([]) ->
    io:format("Se detuvieron todas las replicas.~n");
stopRep([Head | Tail]) ->
    case whereis(Head) of
        undefined ->
            io:format("Replica [~p] not found.~n", [Head]);
        Pid ->
            exit(Pid, normal),
            unregister(Head),
            io:format("Replica [~p] stopped.~n", [Head])
    end,
    stopRep(Tail).

getFamily(Atom) ->
    String = atom_to_list(Atom),
    list_to_atom(hd(string:tokens(String, "-"))).

sendPutTo([Last], Src, Key, Value, Timestamp) ->
    Last ! {put, Src, Key, Value, Timestamp};
sendPutTo([Head | Tail], Src, Key, Value, Timestamp) ->
    Head ! {put, Src, Key, Value, Timestamp},
    sendPutTo(Tail, Src, Key, Value, Timestamp).

sendDelTo([Last], Src, Key, Timestamp) ->
    Last ! {remove, Src, Key, Timestamp};
sendDelTo([Head | Tail], Src, Key, Timestamp) ->
    Head ! {remove, Src, Key, Timestamp},
    sendDelTo(Tail, Src, Key, Timestamp).

coordInit([], Accepted, Src, one) ->
    Src ! {ok, Accepted};
coordInit(Pending, Accepted, Src, one) ->
    receive
        {ok, ReplicName} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    NewList = lists:delete(ReplicName, Pending),
                    NewAcc = [ReplicName | Accepted],
                    coordInit(NewList, NewAcc, Src, one);
                false ->
                    coordInit(Pending, Accepted, Src, one)
            end;
        {error, ReplicName} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    io:format("Error by ~p~n", [ReplicName]),
                    NewList = lists:delete(ReplicName, Pending),
                    NewAcc = [ReplicName | Accepted],
                    coordInit(NewList, NewAcc, Src, one);
                false ->
                    coordInit(Pending, Accepted, Src, one)
            end
    end;
coordInit(Pending, Accepted, Src, quorum) ->
    case length(Accepted) >= length(Pending) of
        true ->
            Src ! {ok, Accepted};
        false ->
            receive
                {ok, ReplicName} ->
                    case lists:member(ReplicName, Pending) of
                        true ->
                            NewList = lists:delete(ReplicName, Pending),
                            NewAcc = [ReplicName | Accepted],
                            coordInit(NewList, NewAcc, Src, quorum);
                        false ->
                            coordInit(Pending, Accepted, Src, quorum)
                    end;
                {error, ReplicName} ->
                    case lists:member(ReplicName, Pending) of
                        true ->
                            io:format("Error by ~p~n", [ReplicName]),
                            NewList = lists:delete(ReplicName, Pending),
                            NewAcc = [ReplicName | Accepted],
                            coordInit(NewList, NewAcc, Src, quorum);
                        false ->
                            coordInit(Pending, Accepted, Src, quorum)
                    end
            end
    end;
coordInit(Pending, Accepted, Src, all) ->
    case length(Pending) == 0 of
        true ->
            Src ! {ok, Accepted};
        false ->
            receive
                {ok, ReplicName} ->
                    case lists:member(ReplicName, Pending) of
                        true ->
                            NewList = lists:delete(ReplicName, Pending),
                            NewAcc = [ReplicName | Accepted],
                            coordInit(NewList, NewAcc, Src, all);
                        false ->
                            coordInit(Pending, Accepted, Src, all)
                    end;
                {error, ReplicName} ->
                    case lists:member(ReplicName, Pending) of
                        true ->
                            io:format("Error by ~p~n", [ReplicName]),
                            NewList = lists:delete(ReplicName, Pending),
                            NewAcc = [ReplicName | Accepted],
                            coordInit(NewList, NewAcc, Src, all);
                        false ->
                            coordInit(Pending, Accepted, Src, all)
                    end
            end
    end.