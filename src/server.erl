%%%-------------------------------------------------------------------
%%% Created : 22. Nov 2020 13:01
%%%-------------------------------------------------------------------
-module(server).
-import(string, [concat/2]).

%% API
-export([start/0, start/1]).

%Startet den server ohne Parameter
start() ->
  start("server.cfg").

%Startet den Server mit einer Logdatei die von Client selber definiert worden ist
start(File) ->
  {ok, ConfigListe} = file:consult(File),
  {ok, SLifetime} = vsutil:get_config_value(latency, ConfigListe),
  {ok, CLifetime} = vsutil:get_config_value(clientlifetime, ConfigListe),
  {ok, Servername} = vsutil:get_config_value(servername, ConfigListe),
  {ok, HBQname} = vsutil:get_config_value(hbqname, ConfigListe),
  {ok, HBQNode} = vsutil:get_config_value(hbqnode, ConfigListe),
  LogNode = string:concat(util:to_String(HBQNode), ".log"),
  LogFile = string:concat("Server", LogNode),
  util:logging(LogFile, io:format("~s", ["Server: server.cfg geöffnet.\n"])),
  %Initialisiere die CMEM
  CMEM = cmem:initCMEM(CLifetime, LogFile),
  util:logging(LogFile, io:format("~s", ["Server: CMEM wurde gestartet.\n"])),
  SPID = spawn(fun() ->
    init(CMEM, 1, SLifetime, LogFile, HBQname, HBQNode) end),
  register(Servername, SPID).

init(CMEM, MessageId, SLifetime, LogFile, HBQname, HBQNode) ->
  util:logging(LogFile, io:format("~s", ["Server: CMEM wurde gestartet.\n"])),
  %Initialisiere die HBQ
  HBQPID = initHBQ(HBQname, HBQNode, LogFile),
  Timer = vsutil:reset_timer(0,SLifetime,{self(),kill}),
  loop(CMEM, HBQPID, MessageId, Timer, SLifetime, LogFile).

loop(CMEM, HBQPID, MessageId, Timer, SLifetime, LogFile) ->
  receive
    %Abfragen einer Nachricht
    {CID, getmessages} ->
      NewTimer = vsutil:reset_timer(Timer,SLifetime,{self(),kill}),
      %Hole die NNr die als nächstes gesendet sein sollte aus der CMEM
      NNr = cmem:getClientNNr(CMEM, CID),
      SendNNr = sendMessage(HBQPID, CID, NNr),
      %Update die CMEM mit der neuen NNr
      NewCMEM = cmem:updateClient(CMEM, CID, SendNNr, LogFile),
      loop(NewCMEM, HBQPID, MessageId, NewTimer, SLifetime, LogFile);
    %Speicher die Nachricht in die HBQ und ggf in die DLQ
    {dropmessage, [INNr, Msg, TSclientout]} ->
      NewTimer = vsutil:reset_timer(Timer,SLifetime,{self(),kill}),
      saveMessage(HBQPID, [INNr, Msg, TSclientout]),
      util:logging(LogFile, io_lib:format("Server: Nachricht ~p wurde in die HBQ gespeichert.\n", [INNr])),
      loop(CMEM, HBQPID, MessageId, NewTimer, SLifetime, LogFile);
    %Liefer eine nächste NNr an den Client
    {CID, getmsgid} ->
      NewTimer = vsutil:reset_timer(Timer,SLifetime,{self(),kill}),
      NextMessageId = MessageId + 1,
      CID ! {nid, NextMessageId},
      util:logging(LogFile, io_lib:format("Server: NNr ~p wurde generiert und wird an ~p gesendet.\n", [NextMessageId, CID])),
      loop(CMEM, HBQPID, NextMessageId, NewTimer, SLifetime, LogFile);
    {CID, listDLQ} ->
      NewTimer = vsutil:reset_timer(Timer,SLifetime,{self(),kill}),
      listDLQ(HBQPID),
      util:logging(LogFile, io_lib:format("Server: Client ~p hat listDLQ aufgerufen.\n", [CID])),
      loop(CMEM, HBQPID, MessageId, NewTimer, SLifetime, LogFile);
    {CID, listHBQ} ->
      NewTimer = vsutil:reset_timer(Timer,SLifetime,{self(),kill}),
      listHBQ(HBQPID),
      util:logging(LogFile, io_lib:format("Server: Client ~p hat listHBQ aufgerufen.\n", [CID])),
      loop(CMEM, HBQPID, MessageId, NewTimer, SLifetime, LogFile);
    {CID, dellHBQ} ->
      NewTimer = vsutil:reset_timer(Timer,SLifetime,{self(),kill}),
      %Terminiere die HBQ
      delHBQ(HBQPID),
      util:logging(LogFile, io_lib:format("Server: Client ~p hat dellHBQ aufgerufen.\n", [CID])),
      loop(CMEM, HBQPID, MessageId, NewTimer, SLifetime, LogFile);
    {_Server, kill} ->
      serverKill(LogFile,CMEM,HBQPID);
    _X ->
      loop(CMEM, HBQPID, MessageId, Timer, SLifetime, LogFile)
  end.


%Abfrage einer Nachricht
%Return die NNr die an Client gesendet worden ist
sendMessage(HBQPID, CID, NNr) ->
  HBQPID ! {self(), {request, deliverMSG, NNr, CID}},
  receive
    {reply, SendNNr} ->
      SendNNr
  end.

%Die HBQ wird initialisiert
initHBQ(HBQName, HBQnode, LogFile) ->
  case net_adm:ping(HBQnode) of
    pong -> timer:sleep(120),
      %Erstelle ein HBQ Prozess
      spawn(HBQnode, fun() -> hbq:startHBQ() end),
      timer:sleep(1000),
      %Sende ein Request an die HBQ dass sie initialisiert sein sollte
      {HBQName, HBQnode} ! {self(), {request, initHBQ}},
      receive
      %Die Antwort der HBQ auf die Initialisierung
        {reply, ok} ->
          util:logging(LogFile, io:format("~s", ["Server: HBQ wurde gestartet.\n"])),
          {HBQName, HBQnode}
      end;
    pang -> % logge einen fehler hier
      util:logging(LogFile, ("Server: HBQ wurde nicht gestartet.\n")),
      nok
  end.


%Speichern einer Nachricht in der HBQ
saveMessage(HBQPID, [INNr, Msg, TSclientout]) ->
  HBQPID ! {self(), {request, pushHBQ, [INNr, Msg, TSclientout]}},
  receive
    {reply, ok} ->
      ok
  end.

%Aktueller Stand der ADT
listDLQ(HBQPID) ->
  HBQPID ! {self(), {request, listDLQ}},
  receive
    {reply, ok} ->
      ok
  end.

%Aktueller Stand der ADT
listHBQ(HBQPID) ->
  HBQPID ! {self(), {request, listHBQ}},
  receive
    {reply, ok} ->
      ok
  end.

%Terminierung der HBQ
delHBQ(HBQPID) ->
  HBQPID ! {self(), {request, dellHBQ}},
  receive
    {reply, ok} ->
      ok
  end.

% Terminiere den Server -> damit auch die CMEM und HBQ -> DLQ
serverKill(LogFile,CMEM,HBQPID) ->
  util:logging(LogFile, io:format("~s", ["Server: Server wird terminiert, zusammen mit der HBQ und CMEM.\n"])),
  cmem:delCMEM(CMEM),
  util:logging(LogFile, io:format("~s", ["Server: CMEM wurde terminiert.\n"])),
  delHBQ(HBQPID),
  util:logging(LogFile, io:format("~s", ["Server: HBQ wurde terminiert.\n"])),
  %Terminiere den Server und sende ein ok zurück
  util:logging(LogFile, io:format("~s", ["Server: Server wurde terminiert.\n"])),
  ok.