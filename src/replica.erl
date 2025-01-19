-module(replica).
-export([start/3,stop/1]).
-define(WAIT,10000).
-include("types.hrl").

% Returns {status,Name} ; status :: ok | name_taken
start(Pid,Name,Group) -> 
    try register(Name,self()), link(Pid) of
      true -> Pid ! {ok,Name}, listen(Name, Group, maps:new())
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
listen(Name, Group, Database) -> 
  receive
    {Pid, Ref, Work = {Operation, Params}, Consistency} -> 
      Queue = resend(Work,Group,Consistency),
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
      case wait(Queue,MyResponse) of  
        timeout -> 
          Pid ! {Ref,timeout}, listen(Name, Group,Database);
        BestResponse -> 
          Pid ! BestResponse#repl{ref = Ref}   
      end,
      if Consistency =:= quorum ->
        listen(Name, operators:shuffle(Group), NewState);  % Randomize group
      true -> 
        listen(Name, Group, NewState)
      end;

    Unknown -> 
      file:write_file(
        "logs.txt", 
        io_lib:fwrite("(~p) INFO: process [~p] received unknown message: ~p\n", [calendar:local_time(), Name,Unknown]),
        [append]
      )
  end.





% Forward work to the group
resend(Work,Group,Consistency) -> resend(Work,Group,Consistency,[]).

resend(_,_,one,Queue) -> Queue;
resend(_,[],_,Queue) -> Queue;

resend(Work, [H | Rest], Consistency, Queue) -> 
  Ref = make_ref(),
  H ! {self(), Ref, Work, one},
  if Consistency =:= quorum andalso length(Rest) > 0  ->
    [_D | T] = Rest,
    resend(Work, T, Consistency, [Ref | Queue]);  % Take two elements per iteration
  true -> 
    resend(Work, Rest, Consistency, [Ref | Queue])
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




