%%%-------------------------------------------------------------------
%%% Created : 21. Nov 2020 09:37
%%%-------------------------------------------------------------------
-module(client).
-import(string, [concat/2]).

%% API
-export([start/0]).

% Startet den Client
start() ->
  {ok, Config} = file:consult("client.cfg"),
  {ok, Clients} = vsutil:get_config_value(clients, Config),
  {ok, LifeTime} = vsutil:get_config_value(lifetime, Config),
  {ok, {Min, SendeIntervall}} = vsutil:get_config_value(sendeintervall, Config),
  {ok, ServerName} = vsutil:get_config_value(servername, Config),
  {ok, ServerNode} = vsutil:get_config_value(servernode, Config),
  LogFile = concat(concat("Client_", integer_to_list(SendeIntervall)), concat(atom_to_list(node()), ".log")),
  case net_adm:ping(ServerNode) of
    pong -> timer:sleep(120),
      %Erstelle ein HBQ Prozess
      Server = {ServerName, ServerNode},
      startClients(LogFile, LifeTime * 1000, Server, Clients, Min, SendeIntervall);
    pang -> % logge einen fehler hier
      util:logging(LogFile, ("Client: Server wurde nicht gestartet.\n")),
      nok
  end.

%% Abbruchbedingung zum starten
startClients(_LogFile, _LifeTime, _Server, 0, _MinIntervall, _Interval) ->
  io:format("Alle Clients wurden gestartet ~n");

%% Starte Loop, da in der client.cfg Anzahl Clients drin steht
startClients(LogFile, LifeTime, Server, Clients, Min, SendeIntervall) ->
  io:format("Starte Clientnummer: ~p ~n ", [Clients]),
  io:format("SendeIntervall: ~p ~n ", [SendeIntervall]),
  spawn(fun() -> init(Clients, LogFile, Server, LifeTime, SendeIntervall * 1000) end),
  AktuellerClient = Clients - 1,
  startClients(concat(concat("Client_", integer_to_list(AktuellerClient)), concat(atom_to_list(node()), ".log")), LifeTime, Server, AktuellerClient, Min, rand:uniform(SendeIntervall - Min + 1) + Min).

%% Client init
init(ClientNR, LogFile, Server, Lifetime, Interval) ->
  ClientBez = lists:flatten(io_lib:format("Client[~p]", [ClientNR])),
  util:logging(LogFile, io_lib:format("~p  laeuft \n", [ClientBez])),
  timer:send_after(Lifetime, {kill}),
  loopRedakteur(ClientBez, Server, 0, Interval, LogFile).

% Wechsle alle 5 Nachrichten zw Leser/Redakteur
loopRedakteur(ClientName, Server, NachrichtenZaehler, SendeIntervall, LogFile) when NachrichtenZaehler > 4 ->
  Server ! {self(), getmsgid},
  receive
    {kill} ->
      clientKill(LogFile);
    {nid, CID} ->
      util:logging(LogFile, io_lib:format("Nachrichtnummer: ~p wird verworfen | Zeitstempel:  ~p  \n", [CID, vsutil:now2string(erlang:timestamp())])),
      util:logging(LogFile, "Wechsle zu Leser \n"),
      loopLeser(ClientName, Server, aendereXRedakteurSekunden(SendeIntervall), LogFile)
  end;

% Entwurf 4
loopRedakteur(ClientName, Server, NachrichtenZaehler, SendeIntervall, LogFile) ->
  Server ! {self(), getmsgid},
  receive
    {kill} ->
      clientKill(LogFile);
    {nid, CID} ->
      vsutil:meinSleep(round(SendeIntervall)),
      dropMessage(ClientName, Server, CID, LogFile),
      loopRedakteur(ClientName, Server, NachrichtenZaehler + 1, SendeIntervall, LogFile)
  end.

% Per Zufall (50%) SleepTime vergroessern/verkleinern
aendereXRedakteurSekunden(SendeIntervall) ->
  aendereXRedakteurSekunden(SendeIntervall, rand:uniform(2)).

% SendeIntervall darf nicht unter 2 Sekunden
aendereXRedakteurSekunden(SendeIntervall, 1) ->
  TmpSendeIntervall = SendeIntervall / 2,
  if
    TmpSendeIntervall < 2000 ->
      2000;
    true ->
      TmpSendeIntervall
  end;

% Zufall Sendezeit verdoppelt sich
aendereXRedakteurSekunden(SendeIntervall, 2) ->
  SendeIntervall * 2.

%% Schicke eine Nachricht an den Server
dropMessage(ClientName, Server, MessageID, LogFile) ->
  Node = node(),
  TS = vsutil:now2string(erlang:timestamp()),
  MessageText = io_lib:format("NNR: ~p ~p - ~p C Out: ~p", [MessageID, ClientName, Node, TS]),
  Server ! {dropmessage, [MessageID, MessageText, erlang:timestamp()]},
  util:logging(LogFile, io_lib:format("Nachrichtnummer: ~p gesendet. C Out: ~p \n", [MessageID, vsutil:now2string(erlang:timestamp())])).

% Entwurf 5
loopLeser(ClientName, Server, SendeIntervall, LogFile) ->
  Server ! {self(), getmessages},
  receive
    {kill} ->
      clientKill(LogFile);
    {reply, Message, false} ->
      printMessage(LogFile, Message, erlang:timestamp()),
      loopLeser(ClientName, Server, SendeIntervall, LogFile);
    {reply, Message, true} ->
      printMessage(LogFile, Message, erlang:timestamp()),
      util:logging(LogFile, "Alle Nachrichten gelesen \n"),
      loopRedakteur(ClientName, Server, 0, SendeIntervall, LogFile)
  end.

%% printMessage: Schreibt auf den Bildschirm und in eine Datei
printMessage(LogFile, Message, CurrentTime) ->
  [MsgNumber, _Msg, ClientOut, HBQin, _DLQin, DLQout] = Message,
  Less = vsutil:lessTS(CurrentTime, DLQout),
  if
    Less == true ->
      util:logging(LogFile, io_lib:format("~p te_Nachricht. C Out ~p HBQ In: ~p DLQ Out: ~p *******; C In: ~p *Nachricht aus der Zukunft fuer Leser: ~p \n", [MsgNumber, vsutil:now2string(ClientOut), vsutil:now2string(HBQin), vsutil:now2string(DLQout), vsutil:now2string(CurrentTime), vsutil:now2stringD(vsutil:diffTS(CurrentTime, DLQout))]));
    true ->
      util:logging(LogFile, io_lib:format("~p te_Nachricht. C Out ~p HBQ In: ~p DLQ Out: ~p *******; C In: ~p \n", [MsgNumber, vsutil:now2string(ClientOut), vsutil:now2string(HBQin), vsutil:now2string(DLQout), vsutil:now2string(CurrentTime)]))
  end.

% Entwurf 6.3
clientKill(LogFile) ->
  util:logging(LogFile, "Client: Terminiere alles! \n"),
  exit(self(), "Terminiere alles").