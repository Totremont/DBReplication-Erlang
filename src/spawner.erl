-module(spawner).
-define(DASH, 45).      % "-" symbol
-define(WAIT,5000).
-define(OFFSET,48).     % Offset to get character encode.
-export([start/2,startWithDummy/2]).

%% Args: FamilyName, N
%% Returns: {ok, Refs, Group} ; Refs :: map() key : name ; value : Ref ({Pid,Ref})
%% and also : {name_taken,Group} | {timeout,Group}

% Interfaces
start(Family,N) -> 
    {ok,Group} = setNames(Family,N),                     % Set proccesses' names
    {ok,Refs} = startProcesses(Group),                   % Spawn processes
    {wait(Refs),Refs,Group}.                            % Wait for confirmation

% Start with dummy process that never returns
startWithDummy(Family,N) -> 
    {ok,Group} = setNames(Family,N),                     
    GroupDummy = [dummy | Group],
    {ok,Refs} = startProcesses(GroupDummy,Group,maps:new()),
    DummyRef = spawn_monitor(replica,dummy,[self()]),
    Refs2 = maps:put(dummy,DummyRef,Refs),   
    {wait(Refs2),Refs2,GroupDummy}.                            

%% Implementations
wait(Refs) -> 
    case maps:size(Refs) of
        0 -> ok;
        _ ->
            receive 
                {ok,Name} -> wait(maps:remove(Name, Refs));
                {name_taken,_} -> abort(Refs), name_taken;
                _ -> wait(Refs)
            after ?WAIT -> 
                abort(Refs),
                timeout
            end
    end.

%% Returns {ok,Acc} ; Acc :: [...] names
setNames(Family,Size) -> setNames(Family, Size,[],1).
setNames(Family,N,Acc,Index) when Index =< N -> 
    Aux = list_to_atom(atom_to_list(Family) ++ [?DASH, (Index + ?OFFSET)]),
    setNames(Family,N,[Aux | Acc],(Index + 1));

setNames(_,_,Acc,_) -> {ok,Acc}.

% Returns {ok,Refs} ; Refs :: map() key : name ; value : Ref ({Pid,Ref})
startProcesses(Group) -> startProcesses(Group,Group,maps:new()).
startProcesses(_,[],Refs) -> {ok,Refs};

startProcesses(Acc,[H | T],Refs) ->
    Ref = spawn_monitor(replica,start,[self(),H,lists:delete(H, Acc)]),
    startProcesses(Acc,T,maps:put(H, Ref, Refs)).

% Kill whatever is alive
abort(Refs) -> maps:foreach(fun(_,{Pid,Ref}) -> demonitor(Ref,[flush]), exit(Pid,kill) end, Refs).