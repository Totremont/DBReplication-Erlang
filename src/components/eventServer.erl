-module(eventServer).
-export([init/0, start/1]).

%% Inicialización del manejador
init() ->
    Pid_msgHandler = spawn(fun() -> msgHandler([]) end),
    register(msgHandler, Pid_msgHandler),
    io:format("Message handler started with PID: ~p~n", [Pid_msgHandler]).

%% Manejador de mensajes
msgHandler(NameList) ->
    receive
        {add, Src, Name} -> %% Cambiado a "add" para coincidir con el mensaje enviado
            case lists:member(Name, NameList) of
                true ->
                    Src ! {error, "Name already exists"},
                    msgHandler(NameList);
                false ->
                    Src ! {ok, "Name added"},
                    NewNameList = [Name | NameList],
                    msgHandler(NewNameList)
            end;
        _ ->
            io:format("Protocol error.~n")
    end.

%% Función para enviar mensajes al manejador
start(Name) ->
    msgHandler ! {add, self(), Name}, %% Enviar el mensaje con el patrón esperado
    receive
        {ok, _} ->
            io:format("[start/1] Added with name [~p].~n", [Name]),
            Pid_newDictionary = spawn(fun() -> dictionaryStart(Name) end),
            register(list_to_atom(Name), Pid_newDictionary);
        {error, _} ->
            io:format("[start/1] Error, name [~p] already exists.~n", [Name])
    end.

dictionaryStart(Name) ->
    io:format("Starting dictionary server ~p~n", [Name]).