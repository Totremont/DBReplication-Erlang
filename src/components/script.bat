@echo off

erlc replica.erl
erlc base_de_datos.erl
erl -eval "base_de_datos:init(), base_de_datos:start(a,5), base_de_datos:put(a,a,one,'a-1'), base_de_datos:put(b,b,one,'a-1'), base_de_datos:say('a-1')."