%%%-------------------------------------------------------------------
%%% Created : 15. Nov 2020 18:01
%%%-------------------------------------------------------------------
-module(cmem).

%% API
-export([initCMEM/2, delCMEM/1, updateClient/4, getClientNNr/2, listCMEM/1, lengthCMEM/1]).

%Initialisiere die CMEM (wird von Server aufgerufen)
%return -> eine leere CMEM
initCMEM(RemTime, Datei) ->
  util:setglobalvar('RemTime', secToMili(RemTime)),
  util:logging(Datei, io:format("~s", ["CMEM: Die CMEM wird erstellt.\n"])),
  [].

%Lösche die CMEM
%return -> ok
delCMEM(_CMEM) ->
  ok.

%Update den Client in der CMEM(Entwurf 5.4.3 updateClient)
%return -> eine neue CMEM
updateClient(CMEM, ClientID, NNr, Datei) ->
  NewADT = checkClientRemTime(util:getglobalvar('RemTime'), vsutil:getUTC(), CMEM, [], Datei),
  update(NewADT, ClientID, NNr, Datei, []).

%Hole die nächst erwartete NachrichtNr eines CLients aus der CMEM(Entwurf 5.4.1 getClientNNr)
%return -> die NachrichtenNr
getClientNNr(CMEM, ClientID) ->
  get(CMEM, ClientID).

%Die Funktion liefer eine Liste der Leser-PIDs mit der gespeicherten Nachrichtennummer (ohne Zeitstempel) zurück
listCMEM(CMEM) ->
  list(CMEM).

%Liefert die Größe bzw. Länge des CMEM zurück.
lengthCMEM(CMEM) ->
  lengthADT(CMEM).

%Client war voher nicht in der CMEM, trage ihm ein
update([], ClientID, NNr, Datei, RestlicheElemente) ->
  util:logging(Datei, io_lib:format("CMEM: Client ~p wurde in die CMEM eingetragen.\n", [ClientID])),
  [{ClientID, vsutil:getUTC(), NNr} | RestlicheElemente];

%Client war voher in der CMEM, update seine NNr und Zeit
update([{ClientID, _TimeDesUpdates, _Wert} | Tail], ClientID, NNr, Datei, RestlicheElemente) ->
  util:logging(Datei, io_lib:format("CMEM: Client ~p wurde in der CMEM geupdated ~p.\n", [ClientID, {ClientID, NNr}])),
  append([{ClientID, vsutil:getUTC(), NNr} | Tail], RestlicheElemente);

update([Head | Tail], ClientID, NNr, Datei, RestlicheElemente) ->
  update(Tail, ClientID, NNr, Datei, [Head | RestlicheElemente]).

%Wenn die CMEM leer ist wird eine 1 zurückgegeben (Client ist nicht in der CMEM)
get([], _ClientID) ->
  1;

%Wenn ein Client in der CMEM gefunden ist wird die NachrichtenNr + 1 zurückgegeben, was das ist die NachrichtenNr die
%der Client als nächstes bekommen sollte
get([{ClientID, _TimeDesUpdates, Wert} | _Tail], ClientID) ->
  Wert + 1;

get([_Head | Tail], ClientID) ->
  get(Tail, ClientID).

list(CMEM) ->
  listIntern(CMEM, []).

%Liefer eine Liste der Leser-PIDs mit der gespeicherten Nachrichtennummer (ohne Zeitstempel) zurück
listIntern([], NewList) ->
  NewList;

listIntern([{ClientID, _TimeDesUpdates, Wert} | Tail], NewList) ->
  listIntern(Tail, [{ClientID, Wert} | NewList]).

lengthADT(CMEM) ->
  lengthList(CMEM).

%Liefert die Größe bzw. Länge des CMEM zurück.
lengthList(List) ->
  lengthList(0, List).

lengthList(Index, []) -> Index;
lengthList(Index, [_Head | Tail]) ->
  lengthList(Index + 1, Tail).

%Löscht alle Clients in der CMEM bei dem die RemTime abgelaufen ist
%return -> CMEM
checkClientRemTime(_RemTime, _TimeNow, [], Rest, _Datei) ->
  reverse(Rest);

%Ein bestimmter Client kann aus der CMEM gelöscht werden
checkClientRemTime(RemTime, TimeNow, [{ClientID, TimeDesUpdates, _Wert} | Tail], Rest, Datei) when TimeNow > TimeDesUpdates + RemTime ->
  util:logging(Datei, io_lib:format("CMEM: Client ~p wurde in der CMEM gelöscht.\n", [ClientID])),
  checkClientRemTime(RemTime, TimeNow, Tail, Rest, Datei);

%Kein Client kann aus der CMEM gelöscht werden
checkClientRemTime(RemTime, TimeNow, [Head | Tail], Rest, Datei) ->
  checkClientRemTime(RemTime, TimeNow, Tail, [Head | Rest], Datei).

secToMili(RemTime) ->
  RemTime * 1000.

append([H | T], Tail) ->
  [H | append(T, Tail)];
append([], Tail) ->
  Tail.

reverse(L) -> reverse(L, []).

reverse([], R) -> R;
reverse([H | T], R) -> reverse(T, [H | R]).