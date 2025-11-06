create or replace
function collect_flow_run_states()
returns table (
	created timestamp with time zone,
	flow_name character varying,
	flow_run_id UUID,
	cnt bigint,
	flow_states state_type[]
)
as $$
begin
return query
select
	min(c.created) as created,
	c.flow_name,
	cast(min(cast(c.flow_run_id as text)) as UUID),
       	count(c.flow_run_id),
       	array_agg(c.flow_state)
from get_flow_run_states() as c
group by c.flow_name
order by created;
end; $$
language plpgsql;
