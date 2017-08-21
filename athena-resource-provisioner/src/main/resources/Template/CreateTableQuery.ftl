CREATE EXTERNAL TABLE `${Name}` (
    <#list Schema as Column>
    `${Column.Name}` ${Column.Type} <#if Column.Comment??> COMMENT '${Column.Comment}'</#if><#if Column_has_next>,</#if>
    </#list>
)
<#if Comment??>COMMENT '${Comment}'</#if>
<#if Partition??>
PARTITIONED BY (
    <#list Partition as Column>
    `${Column.Name}` ${Column.Type} <#if Column.Comment??> COMMENT '${Column.Comment}'</#if><#if Column_has_next>,</#if>
    </#list>
)
</#if>
<#if RowFormat??>
ROW FORMAT SERDE '${RowFormat.SerDe}'
WITH SERDEPROPERTIES (
    <#list RowFormat.Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
    </#list>
)
</#if>
<#if StoredAs??>STORED AS ${StoredAs}</#if>
LOCATION '${Location}'
<#if Properties??>
TBLPROPERTIES (
    <#list Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
    </#list>
);
</#if>
