-module(base_de_datos).

-export([init/0, start/2, say/1, stop/0, stop/1]).

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
            msgHandler(maps:new())
    end.

create(_, 0, List) ->
    io:format("[create/2] Databases created and registered.~n"),
    List;
create(Name, N, List) when N > 0 ->
    ReplicName = list_to_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(N)),
    Pid_newDb = spawn(fun() -> dbInit() end),
    register(ReplicName, Pid_newDb),
    io:format("[create/2] Database [~p] created with PID: ~p.~n", [ReplicName, Pid_newDb]),
    create(Name, N-1, [ReplicName | List]).

dbInit() ->
    receive
        {_} ->
            io:format("Hola mundo! Soy el PID [~p] y me llegó un mensaje", [self()]),
            dbInit()
    end.

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

say(Replica) -> % Es para probar que las replicas se encuentren activas, say(replica, mensaje) donde replica es un LIST! importante!
    list_to_atom(Replica) ! {""}.

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