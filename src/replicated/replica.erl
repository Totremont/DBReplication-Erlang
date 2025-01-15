-module(replica).
-export([start/3]).
-define(WAIT,10000).

% Every function returns {Ref,Response,Timestamp}
% Response is usually the database whereas Timestamp is when the execution finished.
start(Pid,Name,Others) -> 
    try 
      register(Name,self()),
      linkNeighbors(Pid, Others)
    catch
      ok -> Pid ! {ok,Name}, listen(Others,maps:new());
      _:_ -> Pid ! name_taken
    end.

linkNeighbors(_,[]) -> throw(ok);
linkNeighbors(Pid, [H | T]) ->
  try link(whereis(H)) of
    _ -> linkNeighbors(Pid, T)
  catch
    _:_ -> Pid ! link_error
  end.

% Consistency = all | quorum | one
listen(Others, Database) -> 
  receive
    {Pid, Ref, put, Data, Consistency} -> 
      Queue = resend({put,Data},Others,Consistency,[]),
      MyResult = {_,NewDB} = operators:put(Data, Database),
      case wait(Queue,{MyResult,calendar:local_time()}) of  
        % Only the principal can return timeout
        timeout -> Pid ! {Ref,timeout}, listen(Others,Database);
        abort -> Pid ! {Ref,abort}, listen(Others,Database);
        % Return best answer
        {BestResult,BestTime} -> Pid ! {Ref,BestResult,BestTime}
      end,
      if Consistency =:= quorum ->
        listen(shuffle(Others),NewDB);  % Randomize neighbors' position
      true -> 
        listen(Others,NewDB)
      end;

      
    {Pid, Ref, delete, Data} -> operators:delete(Data, Database);
    {Pid, Ref, get,Key} -> operators:get(Key, Database);
    {Pid, Ref, size} -> operators:size(Database)
  end.


% Returns {Response, BestTime} | abort | timeout
wait([],{Response,BestTime}) -> {Response,BestTime};
wait(not_a_queue,{Response,BestTime}) -> abort;
wait(_Dest = [H | T], {CurrRes,CurrTime}) -> 
  receive
    {H,NewRes,NewTime} ->
      case getNewerDate(CurrTime, NewTime) of
        CurrTime -> wait(T,{CurrRes,CurrTime});
        NewTime -> wait(T,{NewRes,NewTime})
      end
  after ?WAIT -> timeout   
  end.

% Consistency handlers
% Returns Message Refs list. 
resend(_,[],_,Refs) -> Refs;
resend(_,_,one,Refs) -> Refs;
resend(_, [_], quorum, Refs) -> Refs;

resend({Operation,Data}, _Others = [H | T], all, Refs) -> 
  Ref = make_ref(),
  try H ! {self(), Ref, Operation, Data, one} of
    _ -> resend({Operation,Data}, T, all, [Ref | Refs])
  catch
    _:_ -> not_a_queue
  end;

resend({Operation,Data}, [H,_D | T], quorum, Refs) ->
  Ref = make_ref(),
  H ! {self(), Ref, Operation, Data, one},
  resend({Operation,Data}, T, quorum, [Ref | Refs]).


% Utils...
getNewerDate(Date1,Date2) ->
  Diff = calendar:datetime_to_gregorian_seconds(Date1) - calendar:datetime_to_gregorian_seconds(Date2),
  if Diff > 0 ->
      Date1;
  true -> Date2 end.

% Change the order of the Others list so quorum calls randomly
shuffle(List) ->
%% Determine the log n portion then randomize the list.
  randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
  randomize(List);
randomize(T, List) ->
  lists:foldl(fun(_E, Acc) ->
    randomize(Acc)
  end, randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
  D = lists:map(fun(A) -> 
      {random:uniform(), A}
    end, List),

  {_, D1} = lists:unzip(lists:keysort(1, D)),
  D1.




