-module(replica).
-export([start/3]).
-record(data,{status = present, value, timestamp}).  %% Status = present | removed

start(Pid,Name,Others) -> 
    try register(Name,self()) of
      _ -> Pid ! {ok,Name}
    catch
      _:_ -> Pid ! name_taken
    end.