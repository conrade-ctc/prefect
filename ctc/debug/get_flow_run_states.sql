create or replace
function get_flow_run_states()
returns table (
	created timestamp with time zone,
	flow_name character varying,
	flow_run_id UUID,
	flow_state state_type
)
as $$
begin
return query
with
    c1 as (
	select flow_run_state.created, flow_run_state.type, flow_run_state.flow_run_id from flow_run_state
    ),
    c2 as (
	select flow_run.id, flow_run.name from flow_run
    )
select c1.created, c2.name, c1.flow_run_id, c1.type 
from
    c1
join
    c2 on c1.flow_run_id = c2.id
order by c1.created;
end; $$
language plpgsql;
