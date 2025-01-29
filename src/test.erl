-module(test).
-include("types.hrl").
-define(WAIT, 8000).
-export([start/0]).

%% Unit tests
start() -> 
    try
    io:format("== Starting Unit test =="),
    io:format("\n1. Initialize coordinator server"),
    ok = main:init(),
    io:format("\n1. ✓ Ok"),

    io:format("\n2. Create a family named 'enode' with 3 members"),
    {ok,start,{enode,['enode-3','enode-2','enode-1']}} 
    = main:start(enode,3),
    io:format("\n2. ✓ Ok"),

    io:format("\n3. Create a family named 'alpha' with 3 members"),
    {ok,start,{alpha,['alpha-3','alpha-2','alpha-1']}} 
    = main:start(alpha,3),
    io:format("\n3. ✓ Ok"),

    io:format("\n4. Store a value with key '3' on 'enode-3' with consistency 'one'"),
    {ok,Ref} = main:put({3,system0,time:now()},'enode-3',one),
    receive
      #repl{ref = {_,Ref}, response = {ok,_,_}} -> ok
    after ?WAIT -> throw(timeout)
    end,
    io:format("\n4. ✓ Ok"),

    io:format("\n5. Store a value with key '5' on 'enode-3' with consistency 'quorum'"),
    {ok,Ref2} = main:put({5,system0,time:now()},'enode-3',quorum),
    receive
      #repl{ref = {_,Ref2}, response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n5. ✓ Ok"),

    io:format("\n6. Store a value with key '5' on 'alpha-1' with consistency 'all'"),
    {ok,Ref3} = main:put({5,system0,time:now()},'alpha-1',all),
    receive
      #repl{ref = {_,Ref3},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n6. ✓ Ok"),

    io:format("\n7. Update key '3' with lower timestamp on 'enode-3' and expect: ko"),  
    {ok,Ref4} = main:put({3,system0,time:daysFromNow(-3)},'enode-3',one),
    receive
      #repl{ref = {_,Ref4},response = {ko,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n7. ✓ Ok"),

    io:format("\n8. Update key '3' with higher timestamp on 'enode-3' and expect: ok"), 
    {ok,Ref5} = main:put({3,system1,time:daysFromNow(1)},'enode-3',one),
    receive
      #repl{ref = {_,Ref5},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n8. ✓ Ok"),

    io:format("\n9. Ask database size on all 'alpha' family members and expect values: 1,1,1"),
    {ok,Ref6} = main:size('alpha-3',one),
    receive
      #repl{ref = {_,Ref6},response = {ok,_,1}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    {ok,Ref7} = main:size('alpha-2',one),
    receive
      #repl{ref = {_,Ref7},response = {ok,_,1}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    {ok,Ref8} = main:size('alpha-1',one),
    receive
      #repl{ref = {_,Ref8},response = {ok,_,1}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n9. ✓ Ok"),

    io:format("\n10. Ask database size on 'enode' family members and expect: 2,1,0"),
    Exp1 = [2,1,0],
    {ok,Ref9} = main:size('enode-1',one),
    Exp2 = 
    receive
      #repl{ref = {_,Ref9},response = {ok,_,Val}} -> lists:delete(Val, Exp1)
      after ?WAIT -> throw(timeout)
    end,
    {ok,Ref10} = main:size('enode-2',one),
    Exp3 = 
    receive
      #repl{ref = {_,Ref10},response = {ok,_,Val2}} -> lists:delete(Val2, Exp2)
      after ?WAIT -> throw(timeout)
    end,
    {ok,Ref11} = main:size('enode-3',one),
    Exp4 = 
    receive
      #repl{ref = {_,Ref11},response = {ok,_,Val3}} -> lists:delete(Val3, Exp3)
      after ?WAIT -> throw(timeout)
    end,
    if Exp4 =:= [] ->
      io:format("\n10. ✓ Ok");
      true -> throw(error)
    end,

    io:format("\n11. Attempt to delete a key '3' on 'alpha-1' and expect: notfound"),
    {ok,Ref12} = main:delete({3,time:now()},'alpha-1',one),
    receive
      #repl{ref = {_,Ref12},response = {notfound,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n11. ✓ Ok"),

    io:format("\n12. Attempt to delete a key '5' on 'alpha-1' with lower timestamp and expect: ko"),
    {ok,Ref13} = main:delete({5,time:daysFromNow(-5)},'alpha-1',one),
    receive
      #repl{ref = {_,Ref13},response = {ko,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n12. ✓ Ok"),

    io:format("\n13. Attempt to delete a key '5' on 'alpha-1' with higher timestamp and consistency 'all' and expect: ok"),
    {ok,Ref14} = main:delete({5,time:daysFromNow(2)},'alpha-1',all),
    receive
      #repl{ref = {_,Ref14},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n13. ✓ Ok"),

    io:format("\n14. Attempt to delete a key '5' on 'alpha-2' with higher timestamp and expect: ko (logically removed)"),
    {ok,Ref15} = main:delete({5,time:daysFromNow(3)},'alpha-2',one),
    receive
      #repl{ref = {_,Ref15},response = {ko,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n14. ✓ Ok"),

    io:format("\n15. Store a key '5' on 'alpha-3' with lower timestamp and expect: ko"),
    {ok,Ref16} = main:put({5,system2,time:daysFromNow(-3)},'alpha-3',one),
    receive
      #repl{ref = {_,Ref16},response = {ko,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n15. ✓ Ok"),

    io:format("\n16. Ask database size on 'alpha-1' with consistency 'quorum' and expect: 0"),
    {ok,Ref17} = main:size('alpha-1',quorum),
    receive
      #repl{ref = {_,Ref17},response = {ok,_,0}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n16. ✓ Ok"),

    io:format("\n17. Get value with key '5' on 'alpha-3' and expect: ko (logically removed)"),
    {ok,Ref18} = main:get(5,'alpha-3',one),
    receive
      #repl{ref = {_,Ref18},response = {ko,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n17. ✓ Ok"),

    io:format("\n18. Get value with key '3' on 'enode-1' and expect: notfound"),
    {ok,Ref20} = main:get(3,'enode-1',one),
    receive
      #repl{ref = {_,Ref20},response = {notfound,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n18. ✓ Ok"),

    io:format("\n19. Get value with key '3' on 'enode-3' and expect: ok"),
    {ok,Ref21} = main:get(3,'enode-3',one),
    receive
      #repl{ref = {_,Ref21},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n19. ✓ Ok"),

    io:format("\n20. Create a family named 'beta' with 3 members and a dummy member (never replies, never sends confirmations)"),
    {ok,start_dummy,{beta,[dummy,'beta-3','beta-2','beta-1']}} 
    = main:startWithDummy(beta,3),
    io:format("\n20. ✓ Ok"),

    io:format("\n21. Store a value with key '8' on 'beta-2' with consistency 'one'"),
    {ok,Ref22} = main:put({8,system0,time:now()},'beta-2',one),
    receive
      #repl{ref = {_,Ref22},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n21. ✓ Ok"),

    io:format("\n22. Get value with key '8' on 'beta-2' with consistency 'one' and expect: ok"),
    {ok,Ref23} = main:get(8,'beta-2',one),
    receive
      #repl{ref = {_,Ref23},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n22. ✓ Ok"),

    io:format("\n23. Get value with key '8' on 'beta-2' with consistency 'all' and expect: timeout"),
    {ok,Ref24} = main:get(8,'beta-2',all),
    receive
      {timeout,Ref24} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n23. ✓ Ok"),

    io:format("\n24. Get value with key '8' on 'beta-3' with consistency 'one' and expect: notfound"),
    {ok,Ref25} = main:get(8,'beta-3',one),
    receive
      #repl{ref = {_,Ref25},response = {notfound,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n24. ✓ Ok"),

    io:format("\n25. Get value with key '8' on 'beta-3' with consistency 'all' and expect: timeout"),
    {ok,Ref26} = main:get(8,'beta-3',all),
    receive
      {timeout,Ref26} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n25. ✓ Ok"),

    io:format("\n26. Stopping family 'alpha'"),
    {ok,stop,{alpha,_}} = main:stop(alpha),
    io:format("\n26. ✓ Ok"),

    io:format("\n27. Ask database size on 'alpha-1' and expect: lider_notfound"),
    {lider_notfound,'alpha-1'} = main:size('alpha-1',quorum),
    io:format("\n27. ✓ Ok"),

    io:format("\n28. Store value with key '6' on 'enode-2' and expect: ok"),
    {ok,Ref28} = main:put({6,system0,time:now()},'enode-2',one),
    receive
      #repl{ref = {_,Ref28},response = {ok,_,_}} -> ok
      after ?WAIT -> throw(timeout)
    end,
    io:format("\n28. ✓ Ok"),

    io:format("\n29. Stop all families."),
    {ok,stop,_} = main:stop(),
    io:format("\n29. ✓ Ok"),

    io:format("\n30. Ask database size on 'enode-2' and expect: lider_notfound"),
    {lider_notfound,'enode-2'} = main:size('enode-2',quorum),
    io:format("\n30. ✓ Ok"),

    io:format("\n31. Ask database size on 'beta-1' and expect: lider_notfound"),
    {lider_notfound,'beta-1'} = main:size('beta-1',quorum),
    io:format("\n31. ✓ Ok"),

    io:format("\n32. Stop coordinator server"),
    ok = main:shutdown(),
    io:format("\n32. ✓ Ok")
    of
        _ -> 
          io:format("\n == Unit test successful. All 32 tests passed. == \n"),
          main:stop(),
          main:shutdown()
    catch
      Type:Res -> 
        io:format("\n == Unit test error. Last test did not pass. == \n"),
        io:format("Error Type: ~p:~p\n",[Type,Res]),
        main:stop(),
        main:shutdown()
    end.
    




    



