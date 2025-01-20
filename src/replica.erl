-module(replica).
-export([start/3,stop/1]).
-define(WAIT,10000).
-include("types.hrl").

% Returns {status,Name} ; status :: ok | name_taken
start(Pid,Name,Group) -> 
    try register(Name,self()), link(Pid) of
      true -> 
        Pid ! {ok,Name}, 
        Timeout = selective:nextTimeout(),
        listen(Name, Group, maps:new(),gb_trees:empty(),Timeout,maps:new())
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
listen(Name, Group, Queue, Table, Timeout, Database) -> 
  receive
    {Pid, Ref, Work = {Operation, Params}, Consistency} -> 
      Pending = resend(Work,Group,Consistency),
      MyResult =
      case Operation of
        put -> operators:put(Params, Database);
        delete -> operators:delete(Params, Database);
        get -> operators:get(Params, Database);
        size -> operators:size(Database)
      end,
      {_,NewState,NewData} = MyResult,
      MyResponse = #repl
      {
        name = Name,
        ref = Ref, 
        response = MyResult,
        timestamp = case NewData of 
          #data{timestamp = SavedTime} -> SavedTime;
          _ -> calendar:local_time() end
      },
      {NewTable, NewQueue, NewOut} = selective:savePending(Pid, MyResponse, Ref, Pending, Queue, Table),
      if Consistency =:= quorum ->
        listen(Name, operators:shuffle(Group), NewState);  % Randomize group
      true -> 
        listen(Name, Group, NewState)
      end;

    Confirm = #repl{} -> selective:onConfirm(Confirm, Table, Queue);

    Unknown -> 
      file:write_file(
        "logs.txt", 
        io_lib:fwrite("(~p) INFO: process [~p] received unknown message: ~p\n", [calendar:local_time(), Name,Unknown]),
        [append]
      )
  after Timeout ->
    selective:purge(Table, Queue)
  end.





% Forward work to the group
resend(Work,Ref,Group,Consistency) -> resend(Work,Ref,Group,Consistency,[]).

resend(_,_,_,one,Queue) -> Queue;
resend(_,_,[],_,Queue) -> Queue;

resend(Work, Ref, [H | Rest], Consistency, Queue) -> 
  H ! {self(), Ref, Work, one},
  if Consistency =:= quorum andalso length(Rest) > 0  ->
    [_D | T] = Rest,
    resend(Work,Ref, T, Consistency, [H | Queue]);  % Take two elements per iteration
  true -> 
    resend(Work,Ref, Rest, Consistency, [H | Queue])
  end.

% Wait for confirmations returns {Response, BestTime} | abort | timeout
wait([],Response) -> Response;
wait([H | T], Response = #repl{timestamp = CurrTime}) -> 
  receive
    Confirm = #repl{ref = H, timestamp = NewTime} ->
      case operators:getNewerDate(CurrTime, NewTime) of
        CurrTime -> wait(T,Response); % Use our response. If dates are equal, current takes precedence.
        NewTime -> wait(T,Confirm)    % Use theirs
      end
  after ?WAIT -> timeout   
  end.




