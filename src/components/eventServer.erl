-module(eventServer).
-export([init/0, start/0, stop/0]).

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

dictionaryStart() ->
    io:format("Starting dictionary server. ~n"),
    loop(). %% Para que no se muera el thread

loop() -> loop().