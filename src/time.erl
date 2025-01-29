-module(time).
-export([now/0,daysFromNow/1]).

% Simple methods to operate with the calendar() module

now() -> calendar:local_time().

daysFromNow(Days) -> 
    calendar:gregorian_seconds_to_datetime(
        calendar:datetime_to_gregorian_seconds(calendar:local_time()) + 60*60*24*Days).

