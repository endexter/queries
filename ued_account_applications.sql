
-------------------------------------------------
-- create staging_unique_account_ids
-------------------------------------------------
-- credit for query used in 
-- staging_unique_account_ids: Anthony Taylor

drop table if exists staging_unique_account_ids;

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
-- pulls product application data and marks 
-- joint accounts

drop table if exists staging_upa;

create temp table staging_upa
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  with apps as (
    with joint_accounts as (
    select
      primary_user_id as user_id,
      'primary' as user_type,
      account_id
    from web_db.joint_account_invitation
    where secondary_user_id is not null

    union

    select
      secondary_user_id as user_id,
      'secondary' as user_type,
      account_id
    from web_db.joint_account_invitation
    where secondary_user_id is not null
  )
    select
       a.*,
       b.name,
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
    left join joint_accounts ja on a.account_id = ja.account_id and a.user_id = ja.user_id
    where a.status != 6
  )
  select a.*, i.unique_account_id
  from apps a
  left join staging_unique_account_ids i
    on (a.user_id = i.user_id
      and a.account_type = i.account_type
    )
);

--select top 500 * from staging_upa;


-------------------------------------------------
-- create staging_average_balances
-------------------------------------------------
-- pulls average balance over first 30, 60, 90
-- days since account creation as well as total
-- deposits and withdrawals over the first 90 days

drop table if exists staging_average_balances;

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
  ),
  period_deposits_withdrawals as (
  with deposits_withdrawals_90 as (
    select unique_account_id,
      ach_deposit_count, atm_deposit_count, check_deposit_count,wire_deposit_count,
      ach_deposit_count + atm_deposit_count + check_deposit_count + wire_deposit_count as total_deposits,
      ach_deposit_amount + atm_deposit_amount + check_deposit_amount + wire_deposit_amount as sum_deposits,
      ach_withdrawal_count, atm_withdrawal_count, check_withdrawal_count,wire_withdrawal_count,
      ach_withdrawal_count + atm_withdrawal_count + check_withdrawal_count + wire_withdrawal_count as total_withdrawals,
      ach_withdrawal_amount + atm_withdrawal_amount + check_withdrawal_amount + wire_withdrawal_amount as sum_withdrawals
    from  bi.dt_account_period_data
    where thirty_day_periods_since_inception < 3
    )
    select unique_account_id,
      sum(total_deposits) as total_deposit_count,
      sum(sum_deposits) as total_deposit_amount,
      sum(total_withdrawals) as total_withdrawal_count,
      sum(sum_withdrawals) as total_withdrawal_amount
    from deposits_withdrawals_90
    group by 1
    order by 1
  )
  select a.*, b.total_deposit_count, b.total_deposit_amount, b.total_withdrawal_count, b.total_withdrawal_amount
  from average_balances a
  left join period_deposits_withdrawals b on a.unique_account_id = b.unique_account_id
  where a.thirty_day_periods_since_inception = 0
);

-- example with user who had some fraudy behavior
-- select top 500 *
-- from staging_average_balances
-- where unique_account_id = 'd100005';


-------------------------------------------------
-- create staging_cm
-------------------------------------------------
-- gets churn model results

drop table if exists staging_cm;

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
-- create staging_first_date_negative_3_days
-------------------------------------------------
-- gets date an account first goes negative
-- and stays negative for at least 3 days

drop table if exists staging_first_date_negative_3_days;

create temp table staging_first_date_negative_3_days
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  with neg_bal as (
    select *,
      max(current_balance) over (order by balance_date rows between 2 preceding and current row) as max_balance
    from web_db.depository_balance
    where current_balance < 0
    order by balance_date
  )
  select concat('d', depository_id) as unique_account_id,
    min(balance_date) - integer '3' as date_negative_3_days
  from neg_bal
  where max_balance < 0
  group by 1
);


-------------------------------------------------
-- create staging_account_staying_negative
-------------------------------------------------
-- get date a summit account went negative and
-- either stay negative went to $0 (since 
-- accounts that are charged off go to $0

drop table if exists staging_account_staying_negative;

create temp table staging_account_staying_negative
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  with all_results as (
    with t as (
      select
        db.depository_id,
        min(db.id) as first_negative_id
      from web_db.depository_balance db
      where db.current_balance < 0
      group by db.depository_id
    )
    select
      t.*,
      t2.current_balance,
      t2.balance_date
    from t
      left join web_db.depository_balance t2 on t.first_negative_id = t2.id
    where not exists(select *
      from web_db.depository_balance db3
      where db3.depository_id = t.depository_id
           and db3.id > t.first_negative_id
           and db3.current_balance > 0)
  )
  select
    concat('d',ar.depository_id) as unique_account_id,
    ar.current_balance as amount_stayed_negative,
    ar.balance_date as date_stayed_negative,
    u.user_email,
    u.user_id,
    u.is_fraud_or_chargeoff
  from all_results ar
    left join web_db.depository d on ar.depository_id = d.id
    left join web_db.user_account ua on d.account_id = ua.account_id
    left join bi.dt_users u on ua.user_id = u.user_id
);


-------------------------------------------------
-- create staging_days_to_first_withdrawal
-------------------------------------------------
-- gets date an account first goes negative
-- and stays negative for at least 3 days

-- drop table if exists staging_days_to_first_withdrawal;

create temp table staging_days_to_first_withdrawal
distkey (unique_account_id)
sortkey (unique_account_id)
as (
  select unique_account_id, min(transaction_date) as first_withdrawal_date
  from bi.dt_summit_transactions
  where transaction_type in ('Wire Transfer - Withdrawal',
    'ACH Withdrawal', 'ATM Withdrawal', 'Check Withdrawal')
  --and unique_account_id = 'd28682'
  group by 1
);


-------------------------------------------------
-- create staging_plaid_token
-------------------------------------------------
-- gets date an account first goes negative
-- and stays negative for at least 3 days

-- drop table if exists staging_plaid_token;

create temp table staging_plaid_token
distkey (user_id)
sortkey (user_id)
as (
  select distinct user_id, plaid_token
  from web_db.user_payment_account
  where plaid_token is not null
);


-------------------------------------------------
-- create public.ued_account_applications
-------------------------------------------------
-- creates a table us users and all there applications that

drop table if exists public.ued_account_applications;

create table public.ued_account_applications
distkey (unique_account_id)
sortkey (unique_account_id)
as (
select 
    upa.user_id,
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
    fdn3.date_negative_3_days,
    asn.date_stayed_negative,
    asn.amount_stayed_negative,
    du.utm_source_clean,
    du.utm_channel_type,
    du.utm_channel_grouping,
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
    ab.debit_card_no_pin_transactions_90,
    ab.total_deposit_count, ab.total_deposit_amount,
    ab.total_withdrawal_count, ab.total_withdrawal_amount,
    da.funding_amount,
    db.verification_type as funding_verification_type,
    da.days_to_first_summit_debit_card_use,
    da.days_to_second_summit_deposit,
    pt.plaid_token
  from staging_upa upa
  left join bi.dt_accounts da on upa.unique_account_id = da.unique_account_id
  left join bi.dt_banks db on da.funding_bank_account_id = db.user_payment_account_id
  left join bi.dt_users du on upa.user_id = du.user_id
  left join staging_cm cm on upa.unique_account_id = cm.unique_account_id
  left join staging_average_balances ab on upa.unique_account_id = ab.unique_account_id
  left join staging_first_date_negative_3_days fdn3 on upa.unique_account_id = fdn3.unique_account_id
  left join staging_account_staying_negative asn on upa.unique_account_id = asn.unique_account_id
  left join staging_plaid_token pt on upa.user_id = pt.user_id
);


-------------------------------------------------
-- create public.ued_id_analytics_20180815
-------------------------------------------------
-- creates table for ID Analytics data pull

drop table if exists public.ued_id_analytics_20180815;

create table public.ued_id_analytics_20180815
distkey (user_id)
sortkey (user_id)
as (
  select
    user_id,
    --unique_account_id,
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
    utm_source_clean,
    utm_channel_type,
    utm_channel_grouping,
    date_negative_3_days as var01,
    date_stayed_negative as var02,
    amount_stayed_negative as var03,
    is_cross_sell as var04,
    is_joint_account as var05,
    churn_score as var06,
    average_balance_30 as var08,
    average_balance_60 as var09,
    average_balance_90 as var10,
    debit_card_pin_transactions_30 as var11,
    debit_card_pin_transactions_60 as var12,
    debit_card_pin_transactions_90 as var13,
    debit_card_no_pin_transactions_30 as var14,
    debit_card_no_pin_transactions_60 as var15,
    debit_card_no_pin_transactions_90 as var16,
    total_deposit_count as var17,
    total_deposit_amount as var18,
    total_withdrawal_count as var19,
    total_withdrawal_amount as var20,
    funding_amount as var21,
    case when funding_verification_type = 'Plaid' then 1
      when funding_verification_type = 'Micro-Deposit' then 0
      else null end  as var22,
    days_to_first_summit_debit_card_use as var23,
    days_to_second_summit_deposit as var24,
    case when date_negative_3_days is not null then 1 else 0 end var25,
    case when date_stayed_negative is not null then 1 else 0 end var26,
    plaid_token as var27
  from (
    select *,
    row_number() over (partition by user_id order by application_date asc) as n
    from public.ued_account_applications
    where initial_account_selected = 'Summit'
  ) where n = 1
  --and application_status in ('approved', 'rejected')
);

