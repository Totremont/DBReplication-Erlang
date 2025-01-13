-module(replica).
-export([start/3]).

start(Pid,Name,Others) -> 
    try register(Name,self()) of
      _ -> Pid ! {ok,Name}
    catch
      _:_ -> Pid ! name_taken
    end.