-module(replica).
-export([start/3,stop/1,dummy/1]).
-include("types.hrl").

% Returns {status,Name} ; status :: ok | name_taken
start(Pid,Name,Group) -> 
    try register(Name,self()), link(Pid) of
      true -> 
        Pid ! {ok,Name}, 
        ConfirmServer = confirm:init(),
        listen(Name, Group, maps:new(), ConfirmServer, maps:new())
    catch
      _:_ -> Pid ! {name_taken, Name}
    end.

stop(Name) ->
  case whereis(Name) of
    undefined -> {notfound, Name};
    Pid -> exit(Pid,kill)
  end.

% Expects: {Pid,Ref,{operation,Params},consistency} ; Consistency :: {all,quorum,one}
% Returns 'repl' :: {ref,Response,Timestamp} ; Response :: {status, Database, optionalResult}
% Or also {Ref,timeout}
listen(Name, Group, Queue, ConfirmServer, Database) -> 

  receive

    {Pid, Ref, Work = {Operation, Params}, Consistency} -> 
      Pending = resend(Work,Ref,Group,Consistency),
      MyResult =
      case Operation of
        put -> operators:put(Params, Database);
        delete -> operators:delete(Params, Database);
        get -> operators:get(Params, Database);
        size -> operators:size(Database);
        inspect -> operators:inspect(Database)
      end,
      {_Status,NewState,NewData} = MyResult,
      MyResponse = #repl
      {
        name = Name,
        ref = Ref, 
        response = MyResult,
        timestamp = case NewData of 
          #data{timestamp = SavedTime} -> SavedTime;
          _ -> calendar:local_time() end  % size() method
      },
      NewQueue = confirm:saveRequest(Pid, MyResponse, Ref, Pending, Queue, ConfirmServer),
      listen(
        Name, if Consistency =:= quorum -> operators:shuffle(Group); true -> Group end,
        NewQueue,ConfirmServer,NewState
      );

    Confirm = #repl{} -> 
      NewQueue = confirm:onConfirm(Confirm, Queue),
      listen(Name, Group,NewQueue,ConfirmServer, Database);

    % A pending queue timeout has expired
    Exp = #exp{} ->
      NewQueue = confirm:purge(Queue, Exp),
      listen(Name, Group,NewQueue,ConfirmServer, Database);

    Unknown -> 
      file:write_file(
        "logs.txt", 
        io_lib:fwrite("(~p) INFO: process [~p] received unknown message: ~p\n", [calendar:local_time(), Name,Unknown]),
        [append]
      )
  end.

% Forward work to the group
resend(Work,Ref,Group,Consistency) -> resend(Work,Ref,Group,Consistency,[]).

resend(_,_,_,one,Queue) -> Queue;
resend(_,_,[],_,Queue) -> Queue;

resend(Work, Ref, [H | Rest], Consistency, Queue) -> 

  Pending = try (H ! {self(), Ref, Work, one}) of 
    _ -> H
  catch 
    _:_ -> nil
  end,
  if Consistency =:= quorum andalso length(Rest) > 0  ->
    [_D | T] = Rest,  % Take two elements per iteration
    resend(Work,Ref, T, Consistency, if Pending =/= nil -> [H | Queue]; true -> Queue end); 
  true -> 
    resend(Work,Ref, Rest, Consistency, if Pending =/= nil -> [H | Queue]; true -> Queue end) 
  end.

dummy(Pid) ->
  try register(dummy,self()), link(Pid) of
    true -> 
      Pid ! {ok,dummy},
      dummyLoop()
  catch
    _:_ -> {name_taken,dummy}
  end.

dummyLoop() ->
  receive
  S -> 
    if S =:= shutdown -> exit(normal);
      true ->
        dummyLoop()
    end
  end.