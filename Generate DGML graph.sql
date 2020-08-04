DROP TABLE IF EXISTS #XEL
SELECT *
INTO #XEL
FROM sys.fn_xe_file_target_read_file('!!!!! PATH TO XEL FILE !!!!!', null, null, null);  

DROP TABLE IF EXISTS #ObjectTypeDict
SELECT T.ObjectType, T.ObjectTypeName INTO #ObjectTypeDict FROM (VALUES
(802018,'Hierarchy'),
(802014,'AttributeHierarchy'),
(802013,'Column'),
(802015,'Partition'),
(802016,'Relationship'),
(801002,'Database')) T(ObjectType,ObjectTypeName)

DROP TABLE IF EXISTS #EventSubclassDict
-- https://docs.microsoft.com/en-us/analysis-services/trace-events/progress-reports-data-columns?view=asallproducts-allversions
SELECT T.EventSubclass, T.EventSubclassName INTO #EventSubclassDict FROM (VALUES
	(1,'Process'),
	(2,'Merge'),
	(3,'Delete'),
	(4,'DeleteOldAggregations'),
	(5,'Rebuild'),
	(6,'Commit'),
	(7,'Rollback'),
	(8,'CreateIndexes'),
	(9,'CreateTable'),
	(10,'InsertInto'),
	(11,'Transaction'),
	(12,'Initialize'),
	(13,'Discretize'),
	(14,'Query'),
	(15,'CreateView'),
	(16,'WriteData'),
	(17,'ReadData'),
	(18,'GroupData'),
	(19,'GroupDataRecord'),
	(20,'BuildIndex'),
	(21,'Aggregate'),
	(22,'BuildDecode'),
	(23,'WriteDecode'),
	(24,'BuildDMDecode'),
	(25,'ExecuteSQL'),
	(26,'ExecuteModifiedSQL'),
	(27,'Connecting'),
	(28,'BuildAggsAndIndexes'),
	(29,'MergeAggsOnDisk'),
	(30,'BuildIndexForRigidAggs'),
	(31,'BuildIndexForFlexibleAggs'),
	(32,'WriteAggsAndIndexes'),
	(33,'WriteSegment'),
	(34,'DataMiningProgress'),
	(35,'ReadBufferFullReport'),
	(36,'ProactiveCacheConversion'),
	(37,'Backup'),
	(38,'Restore'),
	(39,'Synchronize'),
	(40,'BuildProcessingSchedule'),
	(41,'Detach'),
	(42,'Attach'),
	(43,'Analyze\EncodeData'),
	(44,'CompressSegment'),
	(45,'WriteTableColumn'),
	(46,'RelationshipBuildPrepare'),
	(47,'BuildRelationshipSegment'),
	(48,'Load'),
	(49,'MetadataLoad'),
	(50,'DataLoad'),
	(51,'PostLoad'),
	(52,'MetadatatraversalduringBackup'),
	(53,'VertiPaq'),
	(54,'Hierarchyprocessing'),
	(55,'Switchingdictionary'),
	(57,'Tabulartransactioncommit'),
	(58,'Sequencepoint'),
	(59,'Tabularobjectprocessing'),
	(60,'Savingdatabase'),
	(61,'Tokenizationstoreprocessing'),
	(63,'Checksegmentindexes'),
	(64,'Checktabulardatastructure'),
	(65,'Checkcolumndataforduplicatesornullvalues')
)   T(EventSubclass,EventSubclassName)

DROP TABLE IF EXISTS #JobGraphNodes
;WITH XEL AS 
(
	SELECT          x.[object_name] as EventType,
					CAST(x.event_data AS xml) EventXML
	FROM            #XEL x 
), JobGraphXML AS
(
	SELECT          CAST(ns.n.value('(.)[1]','VARCHAR(MAX)') AS XML) [XML]
	FROM            XEL xe
	CROSS APPLY		xe.EventXML.nodes('event//data[@name="JobGraphXml"]//value/.') AS ns(n)
	WHERE			xe.EventType IN ('TabularJobGraph')
)
SELECT			ns.n.value('(@NodeID)[1]','VARCHAR(20)') NodeID,
				ns.n.value('(@ObjectID)[1]','VARCHAR(20)') ObjectID,
				ns.n.value('(@ObjectType)[1]','VARCHAR(50)') ObjectType,
				ns.n.value('(@NodeKind)[1]','VARCHAR(20)') NodeKind,
				ns.n.value('(@ObjectLabel)[1]','VARCHAR(255)') ObjectLabel,
				ns.n.value('(@JobType)[1]','VARCHAR(50)') JobType
INTO			#JobGraphNodes
FROM			JobGraphXML g
CROSS APPLY		g.XML.nodes('JobGraph//Nodes//Node//.') AS ns(n)

DROP TABLE IF EXISTS #JobGraphLinks
;WITH XEL AS 
(
	SELECT          x.[object_name] as EventType,
					CAST(x.event_data AS xml) EventXML
	FROM            #XEL x 
), JobGraphXML AS
(
	SELECT          CAST(ns.n.value('(.)[1]','VARCHAR(MAX)') AS XML) [XML]
	FROM            XEL xe
	CROSS APPLY		xe.EventXML.nodes('event//data[@name="JobGraphXml"]//value/.') AS ns(n)
	WHERE			xe.EventType IN ('TabularJobGraph')
)
SELECT			ns.n.value('(@Sup)[1]','VARCHAR(20)') [Target],
				ns.n.value('(@Dep)[1]','VARCHAR(20)') [Source]
INTO			#JobGraphLinks
FROM			JobGraphXML g
CROSS APPLY		g.XML.nodes('JobGraph//Links//Link//.') AS ns(n)


DROP TABLE IF EXISTS #PROCESSED
;WITH XEL AS 
(
	SELECT          x.[object_name] as EventType,
					CAST(x.event_data AS xml) EventXML
	FROM            #XEL x 
)

SELECT          xe.EventType,
                xe.EventXML.value('(event//data[@name="EventClass"]//value)[1]','VARCHAR(255)')  AS EventClassValue,
                xe.EventXML.value('(event//data[@name="EventClass"]//text)[1]','VARCHAR(255)')  AS EventClassName,
				xe.EventXML.value('(event//data[@name="EventSubclass"]//value)[1]','VARCHAR(255)')  AS EventSubClass,
				xe.EventXML.value('(event//action[@name="attach_activity_id"]//value)[1]','VARCHAR(255)')  AS AttachActivityId,
				xe.EventXML.value('(event//action[@name="attach_current_activity_id"]//value)[1]','VARCHAR(255)') AS AttachCurrentActivityId,
                xe.EventXML.value('(event//data[@name="StartTime"]//value)[1]','VARCHAR(255)')  AS StartTime,
                xe.EventXML.value('(event//data[@name="EndTime"]//value)[1]','VARCHAR(255)')  AS EndTime,
                xe.EventXML.value('(event//data[@name="Duration"]//value)[1]','VARCHAR(255)')  AS Duration,
                xe.EventXML.value('(event//data[@name="ObjectType"]//value)[1]','VARCHAR(255)')  AS ObjectType,
                xe.EventXML.value('(event//data[@name="TextData"]//value)[1]','VARCHAR(512)')  AS TextData,
                xe.EventXML.value('(event//data[@name="ObjectReference"]//value)[1]','VARCHAR(2048)')  AS ObjectReference,
                xe.EventXML.value('(event//data[@name="ObjectPath"]//value)[1]','VARCHAR(2048)')  AS ObjectPath,
                xe.EventXML.value('(event//data[@name="ObjectID"]//value)[1]','VARCHAR(2048)')  AS ObjectID,
                xe.EventXML.value('(event//data[@name="ObjectName"]//value)[1]','VARCHAR(2048)')  AS ObjectName,
                xe.EventXML.value('(event//data[@name="DatabaseName"]//value)[1]','VARCHAR(2048)')  AS DatabaseName
INTO            #PROCESSED
FROM            XEL xe
WHERE			xe.EventType IN ('ProgressReportEnd')


DROP TABLE IF EXISTS #NODES
;WITH [ExExplained] AS 
(
	SELECT			P.DatabaseName,
					COALESCE(P.[2],NULLIF(P.ObjectName ,''),P.DatabaseName) AS ModelName,
					COALESCE(P.[3],NULLIF(P.ObjectName ,''),P.DatabaseName) AS ModelElement,
					CASE WHEN P.ObjectName = 'Partition' THEN 
						CASE WHEN P.EventSubclassName = 'CompressSegment' AND  CHARINDEX(''' for the ''', P.TextData) - CHARINDEX('of column ''',  P.TextData) - 11 > 0 THEN SUBSTRING(P.TextData,11+CHARINDEX('of column ''',  P.TextData),CHARINDEX(''' for the ''', P.TextData) - CHARINDEX('of column ''',  P.TextData) - 11) 
								WHEN P.EventSubclassName IN ('VertiPaq','ExecuteSQL','ReadData','Process','Tabularobjectprocessing','Analyze\EncodeData') THEN P.[3]
						END ELSE ISNULL(NULLIF(P.ObjectName,''),P.DatabaseName) END AS ObjectName,
					ISNULL(P.ObjectTypeName,'Database') AS ObjectTypeName,
					P.TextData,
					P.EventSubclassName,
					CAST(CEILING(10000.0 * p.Duration / MAX(p.Duration) OVER()) AS VARCHAR(20)) Duration,
					--P.ObjectType,
					--P.ObjectReference,
					--P.ObjectPath,
					P.ObjectID
	FROM (
		SELECT      p.TextData,
					p.StartTime,
					p.EndTime,
					es.EventSubclassName,
					p.ObjectType,
					et.ObjectTypeName,
					p.ObjectReference,
					p.ObjectPath,
					p.ObjectID,
					p.ObjectName,
					p.DatabaseName,
					CAST(CAST(p.Duration AS BIGINT) /10000.0 AS FLOAT) Duration,
					o.value ObjectPathDetail,
					ROW_NUMBER() OVER(PARTITION BY p.AttachActivityId,p.ObjectPath ORDER BY (SELECT 0)) ObjectPathDetailSeq
		FROM        #PROCESSED p
		LEFT JOIN	#EventSubclassDict es
		ON			p.EventSubClass = es.EventSubclass
		LEFT JOIN	#ObjectTypeDict et
		ON			p.ObjectType = et.ObjectType
		CROSS APPLY STRING_SPLIT(p.ObjectPath, '.') o
		WHERE       p.EventType = 'ProgressReportEnd'
	) T
	PIVOT  
	(  
		MAX(ObjectPathDetail) FOR ObjectPathDetailSeq IN ([1], [2], [3], [4])  
	) AS P
)
SELECT			P.DatabaseName,
				P.ModelName,
				P.ModelElement,
				P.ObjectName,
				CASE WHEN P.ModelElement = P.ObjectName AND P.ObjectTypeName = 'Partition' THEN 'Table' ELSE P.ObjectTypeName END ObjectTypeName,
				--P.TextData,
				P.EventSubclassName,
				SUM(CAST(p.Duration AS BIGINT)) Duration,
				P.ObjectID
INTO			#NODES 
FROM			[ExExplained] P
WHERE			P.ObjectTypeName != 'Database'
GROUP BY		P.DatabaseName,
				P.ModelName,
				P.ModelElement,
				P.ObjectName,
				CASE WHEN P.ModelElement = P.ObjectName AND P.ObjectTypeName = 'Partition' THEN 'Table' ELSE P.ObjectTypeName END,
				--P.TextData,
				P.EventSubclassName,	
				P.ObjectID

DROP TABLE IF EXISTS #JobNodes
;WITH [Nodes] AS
(
	SELECT			P.NodeID,
					P.ObjectID, 
					P.NodeKind, 
					P.ObjectLabel, 
					P.ObjectType,
					P.JobType,
					REPLACE(P.[1],']','') [1],
					REPLACE(CASE WHEN P.[2]='Partition]' THEN NULL ELSE P.[2] END,']','') [2],
					REPLACE(CASE WHEN P.[3]='Partition]' THEN NULL ELSE P.[3] END,']','') [3]
	FROM (
		SELECT			s.NodeID, 
						s.ObjectID, 
						s.NodeKind, 
						s.ObjectLabel, 
						s.ObjectType,
						s.JobType,
						o.value ObjectPathDetail,
						ROW_NUMBER() OVER(PARTITION BY s.ObjectID, s.ObjectType, s.NodeID, s.ObjectLabel ORDER BY (SELECT 0)) ObjectPathDetailSeq
		FROM			#JobGraphNodes s
		CROSS APPLY		STRING_SPLIT(s.ObjectLabel, '[') o
	) T
	PIVOT  
	(  
		MAX(ObjectPathDetail) FOR ObjectPathDetailSeq IN ([1], [2], [3])  
	) AS P
)
SELECT			n.NodeID,
				n.NodeKind,
				n.JobType,
				n.ObjectID,
				n.ObjectLabel, 
				CASE WHEN n.ObjectType = 'ColumnPartitionStorage' THEN 'Partition' 
					 WHEN n.JobType = 'Process' AND n.ObjectType ='Column' THEN 'Partition'	
					 WHEN n.ObjectType = 'Table' AND n.JobType = 'Placeholder' THEN 'TablePlaceholder'
					 ELSE n.ObjectType END ObjectTypeName,
				n.[1] AS ModelElement,
				CASE WHEN n.ObjectType IN ('Relationship','Table') THEN n.[1] ELSE n.[2] END AS ObjectName,
				n.[3] AS SubObjectName
INTO			#JobNodes
FROM			[Nodes] n

DROP TABLE IF EXISTS #Lookup
SELECT			n.DatabaseName,
				n.ModelName,
				n.ModelElement,
				n.ObjectName,
				n.Duration,
				CASE WHEN n.ObjectTypeName = 'Table' THEN j.ObjectLabel+'['+n.EventSubclassName+']' ELSE j.ObjectLabel END AS ObjectLabel,
				j.SubObjectName,
				j.ObjectID,
				j.NodeID,
				CASE WHEN n.ObjectTypeName = 'Table' THEN ROW_NUMBER() OVER(PARTITION BY n.ModelName,n.ModelElement,n.ObjectName,j.ObjectID,j.NodeID ORDER BY (SELECT 0)) + 1000000 * (CAST(j.NodeID AS INT)+SIGN(CAST(j.NodeID AS INT))-1) END AS NewNodeID,
				CASE WHEN n.ObjectTypeName = 'Table' THEN (SIGN((ROW_NUMBER() OVER(PARTITION BY n.ModelName,n.ModelElement,n.ObjectName,j.ObjectID,j.NodeID ORDER BY (SELECT 0)) - 1))) * (CAST(j.NodeID AS INT)+SIGN(CAST(j.NodeID AS INT))-1) * 1000000 +  ROW_NUMBER() OVER(PARTITION BY n.ModelName,n.ModelElement,n.ObjectName,j.ObjectID,j.NodeID ORDER BY (SELECT 0)) - 1 + (1+SIGN(1-(ROW_NUMBER() OVER(PARTITION BY n.ModelName,n.ModelElement,n.ObjectName,j.ObjectID,j.NodeID ORDER BY (SELECT 0))))) * CAST(j.NodeID AS INT) END AS NewParentNodeID,
				n.ObjectTypeName,
				n.EventSubclassName,
				j.NodeKind,
				CASE WHEN n.ObjectTypeName = 'Table' THEN n.EventSubclassName ELSE j.JobType END AS JobType
INTO			#Lookup
FROM			#NODES n 
LEFT JOIN		#JobNodes j
ON				n.ObjectTypeName = j.ObjectTypeName
AND				n.ModelElement = j.ModelElement
AND				n.ObjectName = j.ObjectName


DROP TABLE IF EXISTS #Containers
;WITH [Container] AS (
	SELECT j.NodeID, j.NewNodeID, j.NewParentNodeID, j.ObjectID  FROM #Lookup j WHERE j.ObjectTypeName = 'Table' AND j.NodeID = j.NewParentNodeID
), [IsPart] AS 
(
	SELECT j.NodeID, j.NewNodeID, j.NewParentNodeID, j.ObjectID  FROM #Lookup j WHERE j.ObjectTypeName = 'Table'
	EXCEPT 
	SELECT j.NodeID, j.NewNodeID, j.NewParentNodeID, j.ObjectID  FROM [Container] j
)
SELECT			CAST(c.NewNodeID AS VARCHAR(255)) AS [Source], 
				CAST(p.NewNodeID AS VARCHAR(255)) AS [Target], 
				CAST('Category="Contains"'  AS VARCHAR(255)) AS Category
INTO			#Containers
FROM			[Container] c 
JOIN			[IsPart] p
ON				c.ObjectID = p.ObjectID


-------------------------------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS #AllNodes
SELECT CAST(j.NodeID AS VARCHAR(255)) NodeID, j.NodeKind, j.JobType, j.ObjectID, j.ObjectLabel, j.ObjectTypeName, j.ModelElement, j.ObjectName, j.SubObjectName, CAST('0' AS VARCHAR(256)) Duration, CAST(NULL AS VARCHAR(255)) [Group] INTO #AllNodes FROM #JobNodes j WHERE NOT EXISTS (SELECT TOP 1 1 FROM #Lookup l WHERE j.NodeID = l.NodeID AND l.ObjectTypeName != 'Table')

UNION ALL

SELECT CAST(j.NodeID AS VARCHAR(255)) NodeID, j.NodeKind, j.JobType, j.ObjectID, j.ObjectLabel, j.ObjectTypeName, j.ModelElement, j.ObjectName, j.SubObjectName, CAST(j.Duration AS VARCHAR(256)) Duration,CAST(NULL AS VARCHAR(255)) [Group] FROM #Lookup j WHERE j.ObjectTypeName != 'Table'

UNION ALL

SELECT CAST(j.NewNodeID AS VARCHAR(255)) NodeID, j.NodeKind, j.JobType, j.ObjectID, j.ObjectLabel, j.EventSubclassName AS ObjectTypeName, j.ModelElement, j.ObjectName, j.SubObjectName, CAST(j.Duration AS VARCHAR(256)) Duration, CASE WHEN j.NodeID = j.NewParentNodeID THEN 'Group="Expanded"' END AS [Group]  FROM #Lookup j WHERE j.ObjectTypeName = 'Table'
-------------------------------------------------------------------


-------------------------------------------------------------------
DROP TABLE IF EXISTS #AllLinks
SELECT CAST(j.Source AS VARCHAR(255)) Source, CAST(j.Target AS VARCHAR(255)) Target, CAST(NULL AS VARCHAR(255)) Category INTO #AllLinks FROM #JobGraphLinks j 

UNION ALL

SELECT  CAST(j.NewNodeID AS VARCHAR(255)),  CAST(j.NewParentNodeID AS VARCHAR(255)), CAST(NULL AS VARCHAR(255)) Category FROM #Lookup j WHERE j.ObjectTypeName = 'Table'

UNION ALL

SELECT c.Source, c.Target, c.Category FROM #Containers c
-------------------------------------------------------------------
-------------------------------------------------------------------


SELECT			CAST('<?xml version="1.0" encoding="utf-8"?>
<DirectedGraph GraphDirection="RightToLeft" Layout="Sugiyama" ZoomLevel="-1" xmlns="http://schemas.microsoft.com/vs/2009/dgml">
  <Nodes>' AS VARCHAR(MAX))

UNION ALL

SELECT			'<Node Id="'+ISNULL(n.NodeID,'')+'" NodeKind="'+n.NodeKind+'" JobType="'+ISNULL(n.JobType,'')+'" ObjectID="'+ISNULL(n.ObjectID,'')+'" ObjectTypeName="'+ISNULL(n.ObjectTypeName,'')+'" Label="'+ISNULL(n.ObjectLabel,'')+'" ModelElement="'+ISNULL(n.ModelElement,'')+'" Duration="'+ISNULL(n.Duration,'')+'" ObjectName="'+ISNULL(n.ObjectName,'')+'" '+ ISNULL(n.[Group],'')+'  />'
FROM			#AllNodes n 

UNION ALL 

SELECT			'</Nodes>'

UNION ALL 

SELECT			'<Links>'

UNION ALL

SELECT			'<Link Source="'+l.Source+'" Target="'+l.Target+'" '+ISNULL(l.Category,'')+' />'
FROM			#AllLinks l

UNION ALL

SELECT			'</Links>'

UNION ALL

SELECT			' <Properties>
    <Property Id="AttachActivityId" DataType="System.String" />
    <Property Id="Bounds" DataType="System.Windows.Rect" />
    <Property Id="DatabaseName" DataType="System.String" />
    <Property Id="Duration" DataType="System.String" />
    <Property Id="Expression" DataType="System.String" />
    <Property Id="GraphDirection" DataType="Microsoft.VisualStudio.Diagrams.Layout.LayoutOrientation" />
    <Property Id="GroupLabel" DataType="System.String" />
    <Property Id="IsEnabled" DataType="System.Boolean" />
    <Property Id="Label" Label="Etykieta" Description="Etykieta, którą można wyświetlić, obiektu Annotable." DataType="System.String" />
    <Property Id="Layout" DataType="System.String" />
    <Property Id="ModelElement" DataType="System.String" />
    <Property Id="ModelName" DataType="System.String" />
    <Property Id="ObjectID" DataType="System.String" />
    <Property Id="ObjectTypeName" DataType="System.String" />
    <Property Id="TargetType" DataType="System.Type" />
    <Property Id="UseManualLocation" DataType="System.Boolean" />
    <Property Id="ValueLabel" DataType="System.String" />
    <Property Id="ZoomLevel" DataType="System.String" />
  </Properties>
  <Styles>

  <Style TargetType="Node" GroupLabel="Duration" ValueLabel="OK">
      <Setter Property="Background" Expression="Color.FromRgb(180, 180 * Duration / 10000, 0)" />
   </Style>
  </Styles>'

UNION ALL

SELECT			'</DirectedGraph>'







