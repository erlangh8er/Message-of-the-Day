%%%-------------------------------------------------------------------
%%% Created : 21. Nov 2020 09:35
%%%-------------------------------------------------------------------
-module(hbqOld).
-import(string, [concat/2]).
%% API
-export([startHBQ/0]).

% Start der HBQ, liest die server.cfg ein
% Die ADT ist nach Nachrichtennummer sortiert.
% Und hat folgende innere Struktur:
% [{NachrichtenID,Nachricht},{NachrichtenID,Nachricht}, ...]
% Entwurf 3.1
startHBQ() ->
  {ok, Config} = file:consult("server.cfg"),
  {ok, HBQName} = vsutil:get_config_value(hbqname, Config),
  {ok, DLQLimit} = vsutil:get_config_value(dlqlimit, Config),
  LogFile = concat("HB-DLQ", concat(atom_to_list(node()), ".log")),
  HBQPID = spawn(fun() -> startHBQ(DLQLimit, LogFile) end),
  register(HBQName, HBQPID),
  ok.

%% Startet die HBQ in einem Process, DLQLimit aus server.cfg gelesen
startHBQ(DLQLimit, LogFile) ->
  receive
    %% Wartet auf request initHBQ vom Server
    {PID, {request, initHBQ}} ->
      %% Erzeugt die DLQ und wartet auf ein reply, ok von ihr
      DLQ = dlq:initDLQ(DLQLimit, LogFile),
      PID ! {reply, ok},

      %% HBQ erfolgreich mit dlq verkuppelt - Inititialisierung beginnt
      util:logging(LogFile, io_lib:format("HBQ: HBQ initialisiert: ~p \n", [PID])),
      HBQ = [],
      %% berechnet das hbqlimit, welches 2/3 der dlq size ist.
      HBQLimit = DLQLimit * 2 / 3,
      loopHBQ(HBQ, DLQ, HBQLimit, LogFile)
  end.

%% HBQ Loop, laeuft solange bis dellHBQ kommt
loopHBQ(HBQ, DLQ, HBQLimit, LogFile) ->
  receive
  %/* Speichern einer Nachricht in der HBQ */
  %HBQ ! {self(), {request,pushHBQ,[NNr,Msg,TSclientout]}}
  %receive {reply, ok}
  % Entwurf 4.3.1
    {PID, {request, pushHBQ, [NNR, Msg, TSclientout]}} ->
      %% ermittle HBQ Laenge
      LenghtHBQ = length(HBQ),
      %% Hole expected NNr
      ENNR = dlq:expectedNr(DLQ),
      % HBQ aufsteigend sortieren
      SHBQ = sortHBQ(HBQ),
      if
      % NNR kleiner als die ENNR = nachricht verwerfen
        NNR < ENNR ->
          %verwefe Nachricht
          util:logging(LogFile, io_lib:format("HBQ: Nachricht verworfen : ~p \n", [NNR])),
          PID ! {reply, ok},
          loopHBQ(SHBQ, DLQ, HBQLimit, LogFile);
        % NNR gleich ENNR = hbq pusht in dlq
        NNR == ENNR ->
          % push2DLQ
          TDLQ = dlq:push2DLQ([ENNR, Msg, TSclientout, vsutil:now2string(erlang:timestamp())], DLQ, LogFile),
          util:logging(LogFile, io_lib:format("HBQ: Nachricht in DLQ : ~p \n", [ENNR])),
          % Wenn in der HBQ noch Elemente sind, dann pruefen ob man mehr als nur ein Element pushen kann
          % Wenn es eine MSGId mit ENNR + 1 gibt, dann kann gepusht werden
          if
            LenghtHBQ > 0 ->
              {HBQ_Tmp_1, DLQ_Tmp_1} = tryToPushMore(SHBQ, TDLQ, LogFile),
              PID ! {reply, ok},
              loopHBQ(HBQ_Tmp_1, DLQ_Tmp_1, HBQLimit, LogFile);
            true ->
              PID ! {reply, ok},
              loopHBQ(SHBQ, DLQ, HBQLimit, LogFile)
          end;
        true ->
          % Füge zu der Msg ein Eingangsstempel
          TSHBQIn = "HBQ In: " ++ util:timeMilliSecond(),
          NewMsg = concat(Msg,TSHBQIn),
          util:logging(LogFile, io_lib:format("HBQ: Nachricht in die HBQ : ~p \n", [NNR])),
          %% Fuege die neue Nachricht in die HBQ
          NewHBQ = [{NNR,[NNR, NewMsg, TSclientout, vsutil:now2string(erlang:timestamp())]}|HBQ],
          %% Ermittle neue HBQ Size
          HBQSize = lengthHBQ(NewHBQ),
          %% Ermittle erstes Element, da es das groesste nicht gesendete ist
          {_MsgNr,[MsgID, _Msg, _TSclientout, _TShbqin]} = firstElement(NewHBQ),
          if
            %% Aufgabestellung:
            %% Wenn in der HBQ von der Anzahl her mehr als 2/3-tel an Nachrichten enthalten sind, als durch die
            %% vorgegebene maximale Anzahl an Nachrichten in der DLQ (Größe der DLQ) stehen können, dann wird,
            %% sofern eine Lücke besteht, diese Lücke zwischen DLQ und HBQ mit genau einer Fehlernachricht geschlossen,
            %% etwa:
            %% "***Fehlernachricht fuer Nachrichtennummern 11 bis 17 um 16.05 18:01:30,580"
            %% indem diese Fehlernachricht in die DLQ eingetragen wird und als Nachrichten-ID die größte fehlende ID
            %% der Lücke erhält (im Beispiel also 17).
            HBQSize >= HBQLimit ->
              %% Msg ID Fehlermeldung
              MsgIDFM = MsgID - 1,
              util:logging(LogFile, io_lib:format("HBQ: Fehlernachricht in die DLQ : ~p \n", [MsgIDFM])),
              %% push2dlq mit oben definierter nachricht
              NewDLQ = dlq:push2DLQ([MsgIDFM, "***Fehlernachricht fuer Nachrichten " ++ util:to_String(ENNR) ++ " bis " ++ util:to_String(MsgID - 1) ++ " um " ++ vsutil:now2string(erlang:timestamp()), {0, 0, 0}, {0, 0, 0}], DLQ, LogFile),
              %% pruefen ob es weitere zusammenhaengende nachrichten existieren, die gepusht werden koennen
              {HBQ_Tmp_1, DLQ_Tmp_1} = tryToPushMore(NewHBQ, NewDLQ, LogFile),
              PID ! {reply, ok},
              loopHBQ(HBQ_Tmp_1, DLQ_Tmp_1, HBQLimit, LogFile);
            true ->
              PID ! {reply, ok},
              loopHBQ(NewHBQ,DLQ,HBQLimit,LogFile)
          end
      end;


    %/* Abfrage einer Nachricht */
    %HBQ ! {self(), {request,deliverMSG,NNr,ToClient}}
    %receive {reply, SendNNr}
    % Entwurf 5.4.2
    {PID, {request, deliverMSG, NNR, ToClient}} ->
      util:logging(LogFile, io_lib:format("HBQ: Beauftrage die DLQ eine Nachricht an den Client ~p zu senden \n", [ToClient])),
      PID ! {reply, dlq:deliverMSG(NNR, ToClient, DLQ, LogFile)},
      loopHBQ(HBQ, DLQ, HBQLimit, LogFile);

    %% Sendet der DLQ den listDLQ Befehl,
    %% damit dessen Nachrichtennummmern geloggt werden.
    {SPID, {request, listDLQ}} ->
      DLQlist = dlq:listDLQ(DLQ),
      %% Beispiel:  HBQ ! {<7016.50.0>, listDLQ}
      %% dlq>>> Content(37): [37,36,35,34,33,32,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
      LenghtDLQ = dlq:lengthDLQ(DLQ),
      util:logging(LogFile, io_lib:format("dlq>>> Content(~p): ~p \n", [LenghtDLQ, DLQlist])),
      %% Bei Erfolg bekommt der Server ein ok.
      SPID ! {reply, ok},
      loopHBQ(HBQ, DLQ, HBQLimit, LogFile);

    %% Loggt die Länge der HBQ,
    %% und die vorhandenen Nachrichtnummern.
    {SPID, {request, listHBQ}} ->
      %% Beispiel:  HBQ ! {<7016.50.0>, listHBQ}
      %% HBQ>>> Content(28): [474,473,472,466,465,464,463,462,461,460,459,458,457,456,455,454,453,452,451,450,449,448,444,443,442,441,438,434]
      Size = lengthHBQ(HBQ),
      List = getNNrs(HBQ),
      util:logging(LogFile, io_lib:format("HBQ>>> Content (~p) : ~p \n", [Size,List])),
      %% Der Server bekommt ein ok als Rückgabe.
      SPID ! {reply, ok},
      loopHBQ(HBQ, DLQ, HBQLimit, LogFile);

    %/* Terminierung der HBQ */
    %HBQ ! {self(), {request,dellHBQ}}
    %receive {reply, ok}
    {PID, {request, dellHBQ}} ->
      dlq:delDLQ(DLQ),
      PID ! {reply, ok},
      utils:log(LogFile, "HBQ beendet PID ~p.", [self()]),
      exit(self(), "HBQ beendet.")
  end.

% sortiere HBQ aufsteigend
% HBQ= [ {MsgID, Msg}, {MsgID, Msg}, ... ]
sortHBQ(L) when length(L) =< 1 ->
  L;
sortHBQ(L) ->
  sortHBQ(L, [], true).

sortHBQ([], L, true) ->
  utils:reverse(L);
sortHBQ([], L, false) ->
  sortHBQ(utils:reverse(L), [], true);
sortHBQ([[NNrP,MsgP,TSclientoutP,TShbqinP],[NNr,Msg,TSclientout,TShbqin]|T], Sorted, _) when NNrP > NNr ->
  sortHBQ([[NNrP,MsgP,TSclientoutP,TShbqinP]|T], [[NNr,Msg,TSclientout,TShbqin]|Sorted], false);
sortHBQ([H|T], Sorted, Halt) ->
  sortHBQ(T, [H|Sorted], Halt).

% HBQ = [{},{}]
tryToPushMore([],DLQ,_Logfile) ->
  {[],DLQ};

tryToPushMore([{NachrID,[NNr,Msg,TSclientout,TShbqin]}],DLQ,Logfile) ->
  DLQNNr = dlq:expectedNr(DLQ),
  if
    NNr == DLQNNr ->
      %% In dem Fall werden die auch an die DLQ weitergeleitet und aus der HBQ entfernt.
      util:logging(Logfile, io_lib:format("HBQ: Nachricht in DLQ : ~p \n", [NNr])),
      NewDLQ = dlq:push2DLQ([NNr,Msg,TSclientout,TShbqin],DLQ,Logfile),
      {[],NewDLQ};
    true ->
      {[{NachrID,[NNr,Msg,TSclientout,TShbqin]}],DLQ}
  end;
tryToPushMore([{NachrID,[NNr,Msg,TSclientout,TShbqin]}|T],DLQ,Logfile) ->
  DLQNNr = dlq:expectedNr(DLQ),
  if
    NNr == DLQNNr ->
      util:logging(Logfile, io_lib:format("HBQ: Nachricht in DLQ : ~p \n", [NNr])),
      NewDLQ = dlq:push2DLQ([NNr,Msg,TSclientout,TShbqin],DLQ,Logfile),
      tryToPushMore(T,NewDLQ,Logfile);
    true ->
      {utils:append([{NachrID,[NNr,Msg,TSclientout,TShbqin]}],T),DLQ}
  end.

% Ermittelt die Nachrichtennummern der HBQ fuer listHBQ
getNNrs([]) ->
  [];
getNNrs(L) ->
  getNNrs(L, []).

getNNrs([[]], A) ->
  A;
getNNrs([{_,[NNr, _, _, _]}], A) ->
  utils:append(A, NNr);
getNNrs([{_,[NNr, _, _, _]} | T], A) ->
  getNNrs(T, utils:append(A, NNr)).

%% Ermittelt die Groesse der HBQ
lengthHBQ(List) ->
  getSize(List,0).

getSize([],Size) ->
  Size;

getSize([_Head|Tail],Size) ->
  getSize(Tail,Size+1).

%% Liefert das erste Element der HBQ zurueck
firstElement([L|[]]) ->
  L;

firstElement([H | _]) ->
  H.