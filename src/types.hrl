%% Database type
-record(data,{status = present, value, timestamp}).  %% Status = present | removed
-record(repl,{name, ref, response, timestamp}). %% response :: {status,Database}