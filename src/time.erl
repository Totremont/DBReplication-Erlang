-module(time).
-export([now/0,daysFromNow/1]).

% Simple methods to operate with the calendar() module

now() -> calendar:local_time().

daysFromNow(Days) -> calendar:gregorian_days_to_date(
    calendar:date_to_gregorian_days(calendar:local_time()) + Days).

