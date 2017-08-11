CREATE EXTERNAL TABLE ${Name} (
<#list Schema as Column>
    `${Column.Name}` ${Column.DataType} <#if Column.Comment??> COMMENT '${Column.Comment}'</#if><#if Column_has_next>,</#if>
</#list>
)
<#if Comment??>
COMMENT '${Comment}'
</#if>
<#if Partition??>
PARTITIONED BY (
<#list Partition as Column>
    `${Column.Name}` ${Column.DataType} <#if Column.Comment??> COMMENT '${Column.Comment}'</#if><#if Column_has_next>,</#if>
</#list>
)
</#if>
ROW FORMAT SERDE '${SerDe.RowFormat}'
<#if SerDe.Properties??>
WITH SERDEPROPERTIES (
<#list SerDe.Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
</#list>
)
STORED AS ${SerDe.StoredAs}
</#if>
LOCATION 's3://${Location}/'
<#if Properties??>
TBLPROPERTIES (
    <#list Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
    </#list>
);
</#if>
