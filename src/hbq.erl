%%%-------------------------------------------------------------------
%%% Created : 25. Nov 2020 20:16
%%%-------------------------------------------------------------------
-module(hbq).
-import(string, [concat/2]).
%% API
-export([startHBQ/0]).

% Entwurf 3.1
% Startet die HBQ und die DLQ
% return ok
startHBQ() ->
  {ok, Config} = file:consult("server.cfg"),
  {ok, HBQname} = vsutil:get_config_value(hbqname, Config),
  {ok, DLQlimit} = vsutil:get_config_value(dlqlimit, Config),
  LogFile = concat("HB-DLQ", concat(atom_to_list(node()), ".log")),
  HBQPID = spawn(fun() -> startHBQ(DLQlimit, LogFile) end),
  register(HBQname, HBQPID),
  ok.

startHBQ(DLQLimit, LogFile) ->
  receive
    {PID, {request, initHBQ}} ->
      DLQ = dlq:initDLQ(DLQLimit, LogFile),
      PID ! {reply, ok},
      util:logging(LogFile, io_lib:format("HBQ: HBQ initialisiert: ~p \n", [PID])),
      util:setglobalvar('DLQLimit', DLQLimit),
      loopHBQ([], DLQ, LogFile)
  end.

loopHBQ(HBQ, DLQ, LogFile) ->
  receive
    % Entwurf 4.3.1
    % Fügt eine Nachricht in die HBQ ein ggf in die DLQ
    % return {reply,ok}
    {SID, {request, pushHBQ, [NNr, Msg, TSclientout]}} ->
      ExpectedNNr = dlq:expectedNr(DLQ),
      SortedHBQ = sortiere(HBQ),
      if
        NNr == ExpectedNNr ->
          NewDLQTemp = putDLQ([NNr, Msg, TSclientout], DLQ, LogFile),
          {NewDLQ, NewHBQ} = tryToPutMore(NewDLQTemp, SortedHBQ, dlq:expectedNr(NewDLQTemp), LogFile);
        NNr < ExpectedNNr ->
          verwerfeNachricht(LogFile, NNr),
          NewDLQ = DLQ,
          NewHBQ = HBQ;
        true ->
          NewHBQTemp = speicherInDieHBQ(HBQ, [NNr, Msg, TSclientout], LogFile),
          NewDLQTemp = fehlerNachricht(DLQ, getSizeHBQ(NewHBQTemp), dlq:expectedNr(DLQ), firstNachricht(NewHBQTemp), LogFile),
          {NewDLQ, NewHBQ} = tryToPutMore(NewDLQTemp, NewHBQTemp, dlq:expectedNr(NewDLQTemp), LogFile)
      end,
      SID ! {reply, ok},
      loopHBQ(NewHBQ, NewDLQ, LogFile);
    % Beauftragt die DLQ eine Nachricht an Client zu senden
    {SID, {request, deliverMSG, NNr, ToClient}} ->
      deliverMSG(SID, NNr, ToClient, DLQ, LogFile),
      loopHBQ(HBQ, DLQ, LogFile);
    % Liefert eine aktuellen Stand von DLQ und speichert es in eine Log Datei
    {SID, {request, listDLQ}} ->
      createListDLQLog(DLQ, LogFile),
      SID ! {reply, ok},
      loopHBQ(HBQ, DLQ, LogFile);
    % Liefert eine aktuellen Stand von HBQ und speichert es in eine Log Datei
    {SID, {request, listHBQ}} ->
      createListHBQLog(HBQ, LogFile),
      SID ! {reply, ok},
      loopHBQ(HBQ, DLQ, LogFile);
    % Terminiert die HBQ und somit auch die DLQ
    {SID, {request, dellHBQ}} ->
      killDLQ(DLQ, LogFile),
      SID ! {reply, ok},
      self() ! {self(), kill};
    % Terminiert die HBQ
    % return ok
    {_Self, kill} ->
      util:logging(LogFile, "HBQ: Die HBQ wurde terminiert \n"),
      ok;
    _X ->
      loopHBQ(HBQ, DLQ, LogFile)
  end.

% Terminiert die DLQ
killDLQ(DLQ, LogFile) ->
  dlq:delDLQ(DLQ),
  util:logging(LogFile, "HBQ: Die DLQ wurde terminiert \n").

% Logt einen aktuellen Stand der HBQ
createListHBQLog(HBQ, LogFile) ->
  Size = getSizeHBQ(HBQ),
  HBQInhalt = getHBQNNr(HBQ),
  util:logging(LogFile, io_lib:format("HBQ>>> Content (~p) : ~p \n", [Size, HBQInhalt])).

% Liefert die Größe der HBQ
getSizeHBQ(HBQ) ->
  getSizeHBQ(HBQ, 0).

getSizeHBQ([], Size) ->
  Size;

getSizeHBQ([{_NachrID, [_NNr, _Msg, _TSclientout, _TShbqin]} | Tail], Size) ->
  getSizeHBQ(Tail, Size + 1).

% Liefert den Inhalt der HBQ ohne der Nachricht (nur NNr)
getHBQNNr(HBQ) ->
  getNNr(HBQ, []).

getNNr([], NNrList) ->
  NNrList;

getNNr([{NachrichtenNr, _Nachricht} | Tail], NNrList) ->
  NewNNrList = append(NNrList, [NachrichtenNr]),
  getNNr(Tail, NewNNrList).

% Logt einen aktuellen Stand der DLQ
createListDLQLog(DLQ, LogFile) ->
  ListDLQ = dlq:listDLQ(DLQ),
  LenghtDLQ = dlq:lengthDLQ(DLQ),
  util:logging(LogFile, io_lib:format("dlq>>> Content(~p): ~p \n", [LenghtDLQ, ListDLQ])).

% Beauftragt die DLQ eine Nachricht an den Client zu senden
deliverMSG(SID, NNr, ToClient, DLQ, LogFile) ->
  util:logging(LogFile, io_lib:format("HBQ: Beauftrage die DLQ eine Nachricht an den Client ~p zu senden \n", [ToClient])),
  SID ! {reply, dlq:deliverMSG(NNr, ToClient, DLQ, LogFile)}.

% Fügt eine Nachricht in die DLQ
% return NNr
putDLQ([NNr, Msg, TSclientout], DLQ, LogFile) ->
  TSHBQIn = concat("HBQ In: ", vsutil:now2string(erlang:timestamp())),
  NewMsg = concat(Msg, TSHBQIn),
  TShbqin = erlang:timestamp(),
  NewDLQ = dlq:push2DLQ([NNr, NewMsg, TSclientout, TShbqin], DLQ, LogFile),
  util:logging(LogFile, io_lib:format("HBQ: Nachricht in DLQ : ~p \n", [NNr])),
  NewDLQ.

% Versucht in die DLQ noch weitere Nachrichten aus der HBQ zu speichern
tryToPutMore(DLQ, [], _ExpectedNNr, _Logfile) ->
  {DLQ, []};

tryToPutMore(DLQ, HBQ, ExpectedNNr, LogFile) ->
  tryToPutMore(DLQ, HBQ, ExpectedNNr, [], LogFile).

tryToPutMore(DLQ, [], _ExpectedNNr, HBQRest, _LogFile) ->
  {DLQ, reverse(HBQRest)};

tryToPutMore(DLQ, [{ExpectedNNr, [NNr, Msg, TSclientout, TShbqin]} | T], ExpectedNNr, HBQRest, LogFile) ->
  util:logging(LogFile, io_lib:format("HBQ: Nachricht in DLQ : ~p \n", [NNr])),
  NewDLQ = dlq:push2DLQ([NNr, Msg, TSclientout, TShbqin], DLQ, LogFile),
  tryToPutMore(NewDLQ, T, dlq:expectedNr(NewDLQ), HBQRest, LogFile);

tryToPutMore(DLQ, [{NNr, [Nr, Msg, TSclientout, TShbqin]} | T], ExpectedNNr, Rest, LogFile) ->
  tryToPutMore(DLQ, T, ExpectedNNr, [{NNr, [Nr, Msg, TSclientout, TShbqin]} | Rest], LogFile).

% Sortiert eine HBQ
sortiere(HBQ) ->
  sortiere(HBQ, []).

sortiere([], Rest) ->
  Rest;
sortiere([{NachrichtenID, Nachricht} | Tail], Rest) ->
  sortiere(Tail, ins({NachrichtenID, Nachricht}, Rest, [])).

ins({NachrichtenID, Nachricht}, [{NachrichtenIDTwo, NachrichtTwo} | TailOne], TailTwo) when NachrichtenIDTwo < NachrichtenID ->
  ins({NachrichtenID, Nachricht}, TailOne, [{NachrichtenIDTwo, NachrichtTwo} | TailTwo]);
ins(Next, TeilOne, TeilTwo) ->
  reverse(TeilTwo) ++ [Next | TeilOne].

% Invertiert die Liste
reverse(L) -> reverse(L, []).
reverse([], R) -> R;
reverse([H | T], R) -> reverse(T, [H | R]).

% Log das verwerfen einer Nachricht
verwerfeNachricht(LogFile, NNr) ->
  util:logging(LogFile, io_lib:format("HBQ: Nachricht verworfen : ~p \n", [NNr])).

% Speichert die Nachricht in die HBQ
% return HBQ
speicherInDieHBQ(HBQ, [NNr, Msg, TSclientout], LogFile) ->
  TSHBQIn = concat("HBQ In: ", vsutil:now2string(erlang:timestamp())),
  NewMsg = concat(Msg, TSHBQIn),
  util:logging(LogFile, io_lib:format("HBQ: Nachricht in die HBQ : ~p \n", [NNr])),
  [{NNr, [NNr, NewMsg, TSclientout, erlang:timestamp()]} | HBQ].

% Die HBQ ist 2/3 der DLQ Size -> schließe die Lücke und speichere eine Fehlernachricht in die DLQ
fehlerNachricht(DLQ, HBQSize, ExpectedNNr, [NNr, _Msg, _TSclientout, _TShbqin], LogFile) ->
  DLQLimit = util:getglobalvar('DLQLimit'),
  case HBQSize >= DLQLimit * 2 / 3 of
    true ->
      FehlerId = NNr - 1,
      util:logging(LogFile, io_lib:format("HBQ: Fehlernachricht in die DLQ : ~p \n", [FehlerId])),
      NewDLQ = dlq:push2DLQ(
        [FehlerId, concat(concat(concat("***Fehlernachricht fuer Nachrichten ", util:to_String(ExpectedNNr)), concat(" bis ", util:to_String(NNr - 1))), concat(" um ", vsutil:now2string(erlang:timestamp()))), {0, 0, 0}, {0, 0, 0}], DLQ, LogFile);
    false -> NewDLQ = DLQ
  end,
  NewDLQ.

firstNachricht([{_MsgNr, [MsgID, Msg, TSclientout, TShbqin]} | []]) ->
  [MsgID, Msg, TSclientout, TShbqin];

firstNachricht([{_MsgNr, [MsgID, Msg, TSclientout, TShbqin]} | _]) ->
  [MsgID, Msg, TSclientout, TShbqin].

append([H | T], Tail) ->
  [H | append(T, Tail)];
append([], Tail) ->
  Tail.