-module(server).
-export([init/0, start/0, stop/0, put/2]).

%% Inicialización del manejador
init() ->
    Pid_msgHandler = spawn(fun() -> msgHandler() end),
    register(msgHandler, Pid_msgHandler),
    io:format("Message handler started with PID: ~p~n", [Pid_msgHandler]).

%% Manejador de mensajes
msgHandler() ->
    receive
        {start, Src} ->
            case whereis(dictionaryServer) of
                undefined ->
                    Pid_newDictionary = spawn(fun() -> dictionaryStart() end),
                    register(dictionaryServer, Pid_newDictionary),
                    Src ! {ok, "Dictionary Server started."};
                _ ->
                    Src ! {error, "Server already exists."}
            end,
            msgHandler();
        {stop, Src} ->
            case whereis(dictionaryServer) of
                undefined ->
                    Src ! {error, "Server not found."};
                Pid ->
                    exit(Pid, normal),
                    unregister(dictionaryServer),
                    Src ! {ok, "Dictionary Server stopped."}
            end,
            msgHandler();
        {put, Src, Key, Value, Timestamp} ->
            dictionaryServer ! {put, Src, Key, Value, Timestamp},
            msgHandler();
        _ ->
            io:format("Protocol error.~n"),
            msgHandler()
    end.


%% Función para enviar mensajes al manejador
start() ->
    msgHandler ! {start, self()}, %% Enviar el mensaje con el patrón esperado
    receive
        {ok, _} ->
            io:format("[start/1] Dictionary server started.~n");
        {error, _} ->
            io:format("[start/1] Error, dictionary server already exists.~n")
    end.

stop() ->
    msgHandler ! {stop, self()},
    receive
        {ok, _} ->
            io:format("[stop/0] Dictionary server stopped.~n");
        {error, _} ->
            io:format("[stop/0] Error, dictionary server not found.~n")
    end.

put(Key, Value) ->
    put(Key, Value, calendar:local_time()).

put(Key, Value, Timestamp) ->
    msgHandler ! {put, self(), Key, Value, Timestamp},
    receive
        {ok, Reply} ->
            io:format("[put/3] Key-Value stored! ~n~p", [Reply]);
        {error, Reply} ->
            io:format("[put/3] error, Key-Value already exists or timestamp stored is greater.~n~p", [Reply])
    end.

dictionaryStart() ->
    io:format("Starting dictionary server. ~n"),
    listen(maps:new()). %% Inicia el diccionario vacio

listen(Map) -> 
    receive
        {put, Src, Key, Value, Timestamp} ->
            case maps:find(Key, Map) of
                error ->
                    NewMap = maps:put(Key, {Value, Timestamp, false}, Map),
                    Src ! {ok, NewMap},
                    listen(NewMap);
                {ok, {_, TimestampStored, Deleted}} ->
                    case calendar:datetime_to_gregorian_seconds(TimestampStored) > calendar:datetime_to_gregorian_seconds(Timestamp) of
                        true ->
                            Src ! {error, Map},
                            listen(Map);
                        false ->
                            NewMap = maps:update(Key, {Value, Timestamp}, Map),
                            Src ! {ok, NewMap},
                            listen(NewMap)
                    end
            end;
        {rem, Src, Key, Timestamp} ->
            case maps:find(Key, Map) of
                error ->
                    Src ! {error, "No key found."};
                {ok, {_, TimestampStored}} ->
                    case calendar:datetime_to_gregorian_seconds(TimestampStored) > calendar:datetime_to_gregorian_seconds(Timestamp) of
                        true ->
                            Src ! {error, Map},
                            listen(Map);
                        false ->
                            NewMap = maps:update(Key, {Value, Timestamp}, Map),
                            Src ! {ok, NewMap},
                            listen(NewMap)
                    end
    end.