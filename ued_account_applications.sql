

-------------------------------------------------
-- create staging_unique_account_ids
-------------------------------------------------
-- Main query credit for creating unique account ids: Anthony

-- drop table if exists staging_unique_account_ids;

create temp table staging_unique_account_ids
distkey (unique_account_id)
sortkey (unique_account_id)
as (
select user_id,
  case
    when depository_id is null then concat('i', investment_id)
    when investment_id is null then concat('d', depository_id)
    else null
      end unique_account_id,
  account_type_id,
  account_id,
  account_type
  from (
    select
      user_id,
      null as depository_id,
      i.id as investment_id,
      case when i.fund_id = 1 and a.account_type_id = 2 then 'Flagship Individual'
      when i.fund_id = 1 and a.account_type_id = 3 then 'Flagship IRA'
      when i.fund_id = 2 and a.account_type_id = 2 then 'Redwood Individual'
      when i.fund_id = 2 and a.account_type_id = 3 then 'Redwood IRA'
      end as account_type,
      case when fund_id = 1 then 2
      when fund_id = 2 then 1 end as product_id,
      account_type_id,
      i.inception_date,
      greatest(0, current_value::numeric(36,4)) as current_balance,
      termination_date,
      fee_pct * greatest(0, i.current_value::numeric(36,4)) / 12.0 as current_monthly_usd_fee_setting,
      fee_pct as current_yearly_percent_fee_setting,
      i.account_id
    from web_db.investment i
    inner join web_db.account a on i.account_id = a.id
    inner join web_db.user_account ua on ua.account_id = a.id
    where i.inception_date is not null

    union all

    select
      user_id,
      d.id as depository_id,
      null as investment_id,
      'Summit' as account_type,
      3 as product_id,
      account_type_id,
      d.inception_date,
      greatest(0, current_balance::numeric(36,4)) as current_balance,
      termination_date,
      d.fee_amount as current_monthly_usd_fee_setting,
      case when greatest(current_balance::numeric(36,4), 0) = 0 then 0
      ELSE d.fee_amount * 12 / (1.0 * greatest(current_balance::numeric(36,4), 0)) end as investment_fee_current,
      d.account_id
    from web_db.depository d
    inner join web_db.account a on d.account_id = a.id
    inner join web_db.user_account ua on ua.account_id = a.id
    where d.inception_date is not null
));


-------------------------------------------------
-- create staging_upa
-------------------------------------------------
-- drop table if exists staging_upa;

create temp table staging_upa
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  with apps as (
    select a.*, b.name,
       case
         when a.product_id = 1 and a.account_type_id = 2 then 'Redwood Individual'
         when a.product_id = 1 and a.account_type_id = 3 then 'Redwood IRA'
         when a.product_id = 2 and a.account_type_id = 2 then 'Flagship Individual'
         when a.product_id = 2 and a.account_type_id = 3 then 'Flagship IRA'
         when a.product_id = 3 then 'Summit'
         else 'Unattributed'
           end account_type,
       case
         when a.status = 0 then 'undefined'
         when a.status = 1 then 'new'
         when a.status = 2 then 'unconfirmed'
         when a.status = 3 then 'submitted'
         when a.status = 4 then 'approved'
         when a.status = 5 then 'rejected'
         when a.status = 6 then 'cancelled'
         when a.status = 7 then 'pending'
           end application_status,
      row_number() over (partition by a.user_id order by a.date_created asc) as application_order,
      case when ja.account_id is null then 0::boolean else 1::boolean end is_joint_account,
      case when ja.account_id is null then 'primary' else ja.user_type end user_account_type
    from web_db.user_product_application a
    left join web_db.product b on a.product_id = b.id
    left join staging_joint_accounts ja on a.account_id = ja.account_id and a.user_id = ja.user_id
    where a.status != 6
  )
  select a.*, i.unique_account_id
  from apps a
  left join staging_unique_account_ids i
    on (a.user_id = i.user_id
      and a.account_type = i.account_type
    )
);



-------------------------------------------------
-- create staging_average_balances
-------------------------------------------------
-- drop table if exists staging_average_balances;

create temp table staging_average_balances
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  with average_balances as (
  select
  unique_account_id,
  thirty_day_periods_since_inception,
  average_balance as average_balance_30,
  lead(average_balance, 1) over (partition by unique_account_id
    order by thirty_day_periods_since_inception asc) as average_balance_60,
  lead(average_balance, 2) over (partition by unique_account_id
    order by thirty_day_periods_since_inception asc) as average_balance_90,
  debit_card_pin_transactions as debit_card_pin_transactions_30,
  lead(debit_card_pin_transactions, 1) over (partition by unique_account_id
    order by thirty_day_periods_since_inception asc) as debit_card_pin_transactions_60,
  lead(debit_card_pin_transactions, 2) over (partition by unique_account_id
    order by thirty_day_periods_since_inception asc) as debit_card_pin_transactions_90,
  debit_card_no_pin_transactions as debit_card_no_pin_transactions_30,
  lead(debit_card_no_pin_transactions, 1) over (partition by unique_account_id
    order by thirty_day_periods_since_inception asc) as debit_card_no_pin_transactions_60,
  lead(debit_card_no_pin_transactions, 2) over (partition by unique_account_id
    order by thirty_day_periods_since_inception asc) as debit_card_no_pin_transactions_90
  from bi.dt_account_period_data
  )
  select *
  from average_balances
  where thirty_day_periods_since_inception = 0
);


-------------------------------------------------
-- create staging_cm
-------------------------------------------------

-- drop table if exists staging_cm;

create temp table staging_cm
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  select
    concat('d', depository_id) as unique_account_id,
    is_cross_sell,
    churn_score
  from public.ucl_churn_model
);


-------------------------------------------------
-- create public.ued_account_applications
-------------------------------------------------
-- creates a table us users and all there applications that

-- drop table if exists public.ued_account_applications;

create table public.ued_account_applications
distkey (unique_account_id)
sortkey (unique_account_id)
as (
select upa.user_id,
  upa.unique_account_id,
  da.account_type,
  upa.name as initial_account_selected,
  upa.is_joint_account,
  upa.user_account_type,
  upa.application_status,
  upa.application_order,
  upa.date_created as application_date,
  da.inception_date as account_creation_date,
  da.termination_date,
  du.first_name,
  du.last_name,
  du.street_line_1,
  du.street_line_2,
  du.city,
  du.state,
  left(du.zip_code,5) as zip_code,
  du.ssn,
  du.date_of_birth,
  du.phone_number,
  du.user_email,
  upa.ip_address,
  da.is_charged_off,
  da.chargeoff_reason,
  da.total_amount_charged_off,
  da.hard_loss_charge_off,
  du.utm_channel_type as channel_affiliate,
  cm.is_cross_sell,
  cm.churn_score,
  ab.average_balance_30,
  ab.average_balance_60,
  ab.average_balance_90,
  ab.debit_card_pin_transactions_30,
  ab.debit_card_pin_transactions_60,
  ab.debit_card_pin_transactions_90,
  ab.debit_card_no_pin_transactions_30,
  ab.debit_card_no_pin_transactions_60,
  ab.debit_card_no_pin_transactions_90
  from staging_upa upa
  left join bi.dt_accounts da on upa.unique_account_id = da.unique_account_id
  left join bi.dt_users du on upa.user_id = du.user_id
  left join staging_cm cm on upa.unique_account_id = cm.unique_account_id
  left join staging_average_balances ab on upa.unique_account_id = ab.unique_account_id
);



-------------------------------------------------
-- create public.ued_id_analytics_20180815
-------------------------------------------------
-- creates table for ID Analytics data pull

-- drop table if exists public.ued_id_analytics_20180815;

create table public.ued_id_analytics_20180815
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  select
    user_id,
    unique_account_id,
    application_status,
    application_date,
    account_creation_date,
    termination_date,
    first_name,
    last_name,
    street_line_1,
    street_line_2,
    city,
    state,
    zip_code,
    ssn,
    date_of_birth,
    phone_number,
    ip_address,
    is_charged_off,
    chargeoff_reason,
    total_amount_charged_off,
    hard_loss_charge_off,
    channel_affiliate
  from (
    select *,
    row_number() over (partition by user_id order by application_date asc) as n
    from public.ued_account_applications
    where initial_account_selected = 'Summit'
  ) where n = 1
  and application_status in ('approved', 'rejected')
);
