%%%-------------------------------------------------------------------
%%% Created : 17. Nov 2020 21:55
%%%-------------------------------------------------------------------
-module(dlq).
-import(string, [concat/2]).
%% API
-export([initDLQ/2, delDLQ/1, expectedNr/1, push2DLQ/3, deliverMSG/4, listDLQ/1, lengthDLQ/1]).

%Initialisiere die DLQ (wird von HBQ aufgerufen)
%return -> eine leere DLQ
initDLQ(Size, Datei) ->
  util:logging(Datei, io_lib:format("DLQ: DLQ wurde mit der Größe ~p erstellt.\n", [Size])),
  {Size, []}.

%Lösche die DLQ
%return -> ok
delDLQ(_Queue) ->
  ok.

%Liefer die Nachricht die als nächstes in die DLQ gespeichert sein sollte
%Ist die DLQ leer wird eine 1 zurückgesendet
%return -> NNr
expectedNr({_Size, []}) ->
  1;

expectedNr({_Size, [{NachrichtenNr, _Nachricht} | _Tail]}) ->
  NachrichtenNr + 1.

%In die DLQ wir eine bestimmte Nachricht gespeichert (Entwurf 4.3.2 push2DLQ)
%return -> DLQ
push2DLQ([NNr, Msg, TSclientout, TShbqin], {Size, List}, Datei) ->
  AktuelleSize = getSize(List, 0),
  case AktuelleSize == Size of
    true -> NewList = deleteOldestMessage(List, Datei);
    false -> NewList = List
  end,
  util:logging(Datei, io_lib:format("DLQ: In die DLQ wurde eine Nachricht mit der NNr ~p hinzugefügt.\n", [NNr])),
  DLQIn = erlang:timestamp(),
  {Size, [{NNr, [NNr, Msg, TSclientout, TShbqin, DLQIn]} | NewList]}.

%Löscht die älteste Nachricht aus der Liste
%return -> list
deleteOldestMessage(List, Datei) ->
  removeLastElement(List, [], Datei).

%Eine bestimmte Nachricht wird an den Client gesendet (Entwurf 5.4.2 deliverMSG)
%return -> die NachrichtNr der Nachricht die an den Client gesendet wird
deliverMSG(MSGNr, ClientPID, Queue, Datei) ->
  case isEmpty(Queue) of
    true ->
      NNr = -1,
      util:logging(Datei, io_lib:format("DLQ: An den CLient ~p wurde eine dummy Nachricht gesendet.\n", [ClientPID])),
      ClientPID ! {reply, [NNr, nokA, 0, 0, 0, 0], true};
    false ->
      [NNr, Msg, TSclientout, TShbqin, TSdlqin] = getMsg(MSGNr, Queue, Queue),
      case isLastNachricht(MSGNr, Queue) of
        true -> Terminated = true;
        false -> Terminated = false
      end,
      util:logging(Datei, io_lib:format("DLQ: Nachricht mit der NNr ~p zum Client ~p gesendet.\n", [NNr, ClientPID])),
      DLQOUT = concat("DLQ Out: ", vsutil:now2string(erlang:timestamp())),
      NewMsg = concat(Msg, DLQOUT),
      ClientPID ! {reply, [NNr, NewMsg, TSclientout, TShbqin, TSdlqin, erlang:timestamp()], Terminated}
  end,
  NNr.

%Hole die Nachricht aus der DLQ wenn die Nachricht nicht existiert hole die nächst größste
%return -> Nachricht
getMsg(MSGNr, {_Size, []}, Queue) ->
  getMsg(MSGNr + 1, Queue, Queue);

getMsg(MSGNr, {_Size, [{MSGNr, Nachricht} | _Tail]}, _Queue) ->
  Nachricht;

getMsg(MSGNr, {_Size, [_Head | Tail]}, Queue) ->
  getMsg(MSGNr, {_Size, Tail}, Queue).

%Überprüfe ob die Nachricht die letzte Nachricht in der DLQ ist
%return -> true wenn dies der Fall ist, sonst false
isLastNachricht(MSGNr, {_Size, [{MSGNr, _Nachricht} | _Tail]}) ->
  true;
isLastNachricht(_MSGNr, {_Size, _Nachricht}) ->
  false.

%Liefert eine Liste der Nachrichtennummern (ohne Nachricht) zurück. Die Reihenfolge entspricht der Reihenfolge in der DLQ
%return -> List
listDLQ({_Size, List}) ->
  getNNr(List, []).

getNNr([], NNrList) ->
  NNrList;

getNNr([{NachrichtenNr, _Nachricht} | Tail], NNrList) ->
  NewNNrList = append(NNrList, [NachrichtenNr]),
  getNNr(Tail, NewNNrList).

lengthDLQ({_Size, List}) ->
  getSize(List, 0).

%Liefert die Größe bzw. Länge der DLQ zurück.
getSize([], Size) ->
  Size;

getSize([_Head | Tail], Size) ->
  getSize(Tail, Size + 1).

%Löscht das letzte Element aus der Liste
%return -> list
removeLastElement([], Rest, _Datei) -> Rest;
removeLastElement([Head | []], Rest, Datei) ->
  {_NID, [MsgID, _Msg, _TSCOut, _TSHBQIn, _TSDLQIn]} = Head,
  util:logging(Datei, io_lib:format("DLQ: Aus der DLQ wurde Nachricht ~p gelöscht.\n", [MsgID])),
  reverse(Rest);
removeLastElement([Head | Tail], Rest, Datei) ->
  removeLastElement(Tail, [Head | Rest], Datei).

%Wenn die Liste leer ist gebe ein true zurück und sonst false
isEmpty([]) ->
  true;

isEmpty(_Queue) ->
  false.

append([H | T], Tail) ->
  [H | append(T, Tail)];
append([], Tail) ->
  Tail.

reverse(L) -> reverse(L, []).

reverse([], R) -> R;
reverse([H | T], R) -> reverse(T, [H | R]).