% Consistencia: 
%    Las replicas no manejan pedidos de manera concurrente, atienden las solicitudes a medidas que les va llegando.
%    Si se realiza una operacion con 'all' si se pueden realizar otras operaciones mientras se espera la respuesta de
%   otras replicas.
%   
%   Creo que es libre de deadlocks por como erlang implementa los receive y además, solo hay deadlocks cuando no se
%  recibe un mensaje y el proceso queda bloqueado en un receive (pero en el código siempre se envian los mensajes correspondientes)


-module(base_de_datos).

-export([init/0, start/2, say/1, stop/0, stop/1, put/4, put/5, del/3, del/4, get/3]).

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
                            CoordPid = spawn(fun() -> coordInit(List, [], [],Src, one, Coord) end),
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
                            CoordPid = spawn(fun() -> coordInit(List, [], [], Src, quorum, Coord) end),
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
                            CoordPid = spawn(fun() -> coordInit(List, [], [], Src, all, Coord) end),
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
                            CoordPid = spawn(fun() -> coordInit(List, [], [], Src, one, Coord) end),
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
                            CoordPid = spawn(fun() -> coordInit(List, [], [], Src, quorum, Coord) end),
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
                            CoordPid = spawn(fun() -> coordInit(List, [], [], Src, all, Coord) end),
                            sendDelTo(List, CoordPid, Key, Timestamp),
                            msgHandler(Names);
                        error ->
                            Src ! {error, "Family not found"},
                            msgHandler(Names)
                    end
            end;
        {get, Src, Key, one, Coord} ->
            case whereis(Coord) of
                undefined ->
                    Src ! {error, "Coordinator doesn't exists."},
                    msgHandler(Names);
                _ ->
                    Family = getFamily(Coord),
                    % Coord ! {put, Src, Key, Value, Timestamp, Consistency, Coord, maps:find(Family, Names)}
                    case maps:find(Family, Names) of
                        {ok, List} when is_list(List) -> 
                            CoordPid = spawn(fun() -> coordInitGet(List, maps:new(), maps:new(), [], Src, one, Coord) end),
                            sendGetTo(List, CoordPid, Key),
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
get(Key, Consistency, Coord) ->
    msgHandler ! {get, self(), Key, Consistency, Coord},
    receive
        {error, notfound} ->
            io:format("[get/3] Not found.~n");
        {error, _} ->
            io:format("[get/3] Error.~n");
        {ko, Timestamp} ->
            io:format("[get/3] Key was deleted on ~p.~n", [Timestamp]);
        {ok, Value, Timestamp} ->
            io:format("[get/3] Ok! Value: ~p~nTimestamp: ~p~n", [Value,Timestamp]);
        _ ->
            io:format("[get/3] Default exit.~n")
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

sendGetTo([Last], Src, Key) ->
    Last ! {get, Src, Key};
sendGetTo([Head | Tail], Src, Key) ->
    Head ! {get, Src, Key},
    sendGetTo(Tail, Src, Key).

coordInit([], Accepted, Rejected, Src, one, Coord) ->
    case lists:member(Coord, Accepted) of
        true ->
            sendCoR(Accepted ++ Rejected, ok),
            Src ! {ok, Accepted};
        false ->
            sendCoR(Accepted ++ Rejected, rollback),
            Src ! {error, Accepted}
    end;
coordInit([], Accepted, Rejected, Src, quorum, _) ->
    case length(Accepted) > length(Rejected) of
        true ->
            sendCoR(Accepted ++ Rejected, ok),
            Src ! {ok, Accepted};
        false ->
            sendCoR(Accepted ++ Rejected, rollback),
            Src ! {error, Accepted}
    end;
coordInit([], Accepted, Rejected, Src, all, _) ->
    case length(Rejected) == 0 of
        true ->
            sendCoR(Accepted ++ Rejected, ok),
            Src ! {ok, Accepted};
        false ->
            sendCoR(Accepted ++ Rejected, rollback),
            Src ! {error, Accepted}
    end;
coordInit(Pending, Accepted, Rejected, Src, Consistency, Coord) ->
    receive
        {ok, ReplicName} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    NewList = lists:delete(ReplicName, Pending),
                    NewAcc = [ReplicName | Accepted],
                    coordInit(NewList, NewAcc, Rejected, Src, Consistency, Coord);
                false ->
                    coordInit(Pending, Accepted, Rejected, Src, Consistency, Coord)
            end;
        {error, ReplicName} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    io:format("Error by ~p~n", [ReplicName]),
                    NewList = lists:delete(ReplicName, Pending),
                    NewRej = [ReplicName | Rejected],
                    coordInit(NewList, Accepted, NewRej, Src, Consistency, Coord);
                false ->
                    coordInit(Pending, Accepted, Rejected, Src, Consistency, Coord)
            end
    end.

coordInitGet([], Ok, Ko, Nf, Src, one, Coord) ->
    case lists:member(Coord, Nf) of
        true ->
            Src ! {error, notfound};
        false ->
            case maps:find(Coord, Ko) of
                {ok, Timestamp} ->
                    Src ! {ko, Timestamp};
                error ->
                    case maps:find(Coord, Ok) of
                        {ok, {Value, Timestamp}} ->
                            Src ! {ok, Value, Timestamp};
                        error ->
                            Src ! {error, ""}
                    end
            end
    end;
coordInitGet(Pending, Ok, Ko, Nf, Src, Consistency, Coord) -> %List: lista, Ok: Map, Ko: Map, Nf: lista
    receive
        {notfound, ReplicName} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    NewList = lists:delete(ReplicName, Pending),
                    NewNf = [ReplicName | Nf],
                    coordInitGet(NewList, Ok, Ko, NewNf, Src, Consistency, Coord);
                false ->
                    coordInitGet(Pending, Ok, Ko, Nf, Src, Consistency, Coord)
            end;
        {ko, ReplicName, Timestamp} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    NewList = lists:delete(ReplicName, Pending),
                    NewKo = maps:put(ReplicName, Timestamp, Ko),
                    coordInitGet(NewList, Ok, NewKo, Nf, Src, Consistency, Coord);
                false ->
                    coordInitGet(Pending, Ok, Ko, Nf, Src, Consistency, Coord)
            end;
        {ok, ReplicName, Value, Timestamp} ->
            case lists:member(ReplicName, Pending) of
                true ->
                    NewList = lists:delete(ReplicName, Pending),
                    NewOk = maps:put(ReplicName, {Value, Timestamp}, Ok),
                    coordInitGet(NewList, NewOk, Ko, Nf, Src, Consistency, Coord);
                false ->
                    coordInitGet(Pending, Ok, Ko, Nf, Src, Consistency, Coord)
            end
    end.









% coordInit(Pending, Accepted, Rejected, Src, quorum) ->
%     case length(Accepted) >= length(Pending) of
%         true ->
%             Src ! {ok, Accepted};
%         false ->
%             receive
%                 {ok, ReplicName} ->
%                     case lists:member(ReplicName, Pending) of
%                         true ->
%                             NewList = lists:delete(ReplicName, Pending),
%                             NewAcc = [ReplicName | Accepted],
%                             coordInit(NewList, NewAcc, Src, quorum);
%                         false ->
%                             coordInit(Pending, Accepted, Src, quorum)
%                     end;
%                 {error, ReplicName} ->
%                     case lists:member(ReplicName, Pending) of
%                         true ->
%                             io:format("Error by ~p~n", [ReplicName]),
%                             NewList = lists:delete(ReplicName, Pending),
%                             NewAcc = [ReplicName | Accepted],
%                             coordInit(NewList, NewAcc, Src, quorum);
%                         false ->
%                             coordInit(Pending, Accepted, Src, quorum)
%                     end
%             end
%     end;
% coordInit(Pending, Accepted, Rejected, Src, all) ->
%     case length(Pending) == 0 of
%         true ->
%             Src ! {ok, Accepted};
%         false ->
%             receive
%                 {ok, ReplicName} ->
%                     case lists:member(ReplicName, Pending) of
%                         true ->
%                             NewList = lists:delete(ReplicName, Pending),
%                             NewAcc = [ReplicName | Accepted],
%                             coordInit(NewList, NewAcc, Src, all);
%                         false ->
%                             coordInit(Pending, Accepted, Src, all)
%                     end;
%                 {error, ReplicName} ->
%                     case lists:member(ReplicName, Pending) of
%                         true ->
%                             io:format("Error by ~p~n", [ReplicName]),
%                             NewList = lists:delete(ReplicName, Pending),
%                             NewAcc = [ReplicName | Accepted],
%                             coordInit(NewList, NewAcc, Src, all);
%                         false ->
%                             coordInit(Pending, Accepted, Src, all)
%                     end
%             end
%     end.

sendCoR([], _) ->
    ok;
sendCoR([Head | Tail], Reply) ->
    Head ! {Reply},
    sendCoR(Tail, Reply).