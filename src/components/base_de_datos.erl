-module(base_de_datos).

-export([init/0, start/2, say/1, stop/1]).

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
                            create(Name, N),
                            Src ! {ok, "Databases created."},
                            NewNames = maps:put(Name, N, Names),
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
                    stopRep(Name, Value),
                    Src ! {ok, ""},
                    NewNames = maps:remove(Name, Names),
                    msgHandler(NewNames)
            end
    end.

create(_, 0) ->
    io:format("[create/2] Databases created and registered.~n");
create(Name, N) when N > 0 ->
    Pid_newDb = spawn(fun() -> dbInit() end),
    register(list_to_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(N)), Pid_newDb),
    io:format("[create/2] Database [~p-~p] created with PID: ~p.~n", [Name, N, Pid_newDb]),
    create(Name, N-1).

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

say(Replica) -> % Es para probar que las replicas se encuentren activas, say(replica, mensaje) donde replica es un LIST! importante!
    list_to_atom(Replica) ! {""}.

stopRep(Name, 0) ->
    io:format("Se detuvieron todas las replicas con el nombre [~p]", [Name]);
stopRep(Name, N) when N > 0 ->
    ReplicaName = list_to_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(N)),
    case whereis(ReplicaName) of
        undefined ->
            io:format("Replica [~p] not found.~n", [ReplicaName]);
        Pid ->
            exit(Pid, normal),
            unregister(ReplicaName),
            io:format("Replica [~p] stopped.~n", [ReplicaName])
    end,
    stopRep(Name, N - 1).