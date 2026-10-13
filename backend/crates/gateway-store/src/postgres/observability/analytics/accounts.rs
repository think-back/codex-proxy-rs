//! Provider 账号用量：完整桶合并账号模型汇总，首尾与 dirty 区间只读完成用量窄事实

use super::query::push_summary_buckets;
use super::worker::account_model_dimensions;
use super::*;
use futures::TryStreamExt;

/// 汇总行只能定位到时间桶，窄事实保留精确开始时间
enum Position {
    Bucket {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    Fact(DateTime<Utc>),
}

#[derive(Default)]
struct ModelUsage {
    state: MetricState,
    latest_fact: Option<DateTime<Utc>>,
    latest_bucket: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

impl AnalyticsSnapshot<'_> {
    pub async fn provider_account_usage(
        &mut self,
        query: ProviderAccountUsageQuery,
    ) -> StoreResult<Vec<ProviderAccountUsageObservation>> {
        if let Some(account_ids) = &query.account_ids {
            validate_account_ids(account_ids)?;
        }
        if query.limit == 0 || usize::from(query.limit) > MAX_ACCOUNT_IDS {
            return Err(invalid("account usage limit must be between 1 and 200"));
        }
        self.require_visible(Some(query.range)).await?;
        if let Some(range) = query.request_bucket_range {
            self.require_visible(Some(range)).await?;
        }
        let mut observations = self.usage_accounts(&query).await?;
        if observations.is_empty() {
            return Ok(observations);
        }
        let account_ids = observations
            .iter()
            .map(|item| item.account_id.clone())
            .collect::<Vec<_>>();

        let mut usage = BTreeMap::<(String, String), ModelUsage>::new();
        self.read_account_models(
            query.range,
            &account_ids,
            |account, model, position, state| {
                let item = usage.entry((account, model)).or_default();
                item.state.merge(&state)?;
                match position {
                    Position::Bucket { start, end } => {
                        if item.latest_bucket.is_none_or(|(latest, _)| start > latest) {
                            item.latest_bucket = Some((start, end));
                        }
                    }
                    Position::Fact(started_at) => {
                        item.latest_fact = item.latest_fact.max(Some(started_at));
                    }
                }
                Ok(())
            },
        )
        .await?;
        let model_last_used = self.model_last_used(&usage).await?;
        let mut request_buckets = if let Some(range) = query.request_bucket_range {
            self.account_request_buckets(range, &account_ids).await?
        } else {
            HashMap::new()
        };

        for observation in &mut observations {
            let mut account = MetricState::default();
            let mut models = Vec::new();
            for ((_, model), item) in usage
                .range((observation.account_id.clone(), String::new())..)
                .take_while(|((account_id, _), _)| *account_id == observation.account_id)
            {
                account.merge(&item.state)?;
                // 上游与请求模型都缺失的完成用量只计入账号合计，与模型明细的展示口径一致
                if model.is_empty() {
                    continue;
                }
                let last_used_at = item
                    .latest_fact
                    .max(
                        model_last_used
                            .get(&(observation.account_id.clone(), model.clone()))
                            .copied(),
                    )
                    .ok_or_else(|| invalid("missing account model last use"))?;
                models.push(ProviderAccountModelUsageObservation {
                    model: model.clone(),
                    request_count: item.state.count("completed_request_count"),
                    success_count: item.state.count("completed_request_count"),
                    input_tokens: item.state.known_count("input_tokens"),
                    output_tokens: item.state.known_count("output_tokens"),
                    cached_input_tokens: item.state.known_count("cached_input_tokens"),
                    cache_write_input_tokens: item.state.known_count("cache_write_input_tokens"),
                    reasoning_output_tokens: item.state.known_count("reasoning_output_tokens"),
                    image_input_tokens: item.state.known_count("image_input_tokens"),
                    image_output_tokens: item.state.known_count("image_output_tokens"),
                    image_request_count: item.state.count("image_request_count"),
                    image_request_failed_count: item.state.count("image_request_failed_count"),
                    total_tokens: item.state.known_count("total_tokens"),
                    cost_coverage: item.state.coverage(),
                    costs: item.state.costs()?,
                    last_used_at,
                });
            }
            models.sort_by(|left, right| {
                right
                    .request_count
                    .cmp(&left.request_count)
                    .then_with(|| left.model.cmp(&right.model))
            });
            // 完成用量都是成功终态，账号卡片的成功数与调用次数同源
            observation.request_count = account.count("completed_request_count");
            observation.success_count = account.count("completed_request_count");
            observation.input_tokens = account.known_count("input_tokens");
            observation.output_tokens = account.known_count("output_tokens");
            observation.cached_input_tokens = account.known_count("cached_input_tokens");
            observation.cache_write_input_tokens = account.known_count("cache_write_input_tokens");
            observation.reasoning_output_tokens = account.known_count("reasoning_output_tokens");
            observation.image_input_tokens = account.known_count("image_input_tokens");
            observation.image_output_tokens = account.known_count("image_output_tokens");
            observation.image_request_count = account.count("image_request_count");
            observation.image_request_failed_count = account.count("image_request_failed_count");
            observation.total_tokens = account.known_count("total_tokens");
            observation.cost_coverage = account.coverage();
            observation.costs = account.costs()?;
            observation.models = models;
            observation.request_buckets = request_buckets
                .remove(&observation.account_id)
                .unwrap_or_default();
        }
        Ok(observations)
    }

    /// 账号身份读取当前配置，最近使用时间按完成用量索引逐账号取一行
    async fn usage_accounts(
        &mut self,
        query: &ProviderAccountUsageQuery,
    ) -> StoreResult<Vec<ProviderAccountUsageObservation>> {
        let mut statement = QueryBuilder::<Postgres>::new(
            "select pa.id, pa.provider_kind, pa.authentication_kind, pa.name, pa.email, pa.plan_type, latest.started_at as last_used_at from provider_accounts pa ",
        );
        // 显式账号即使区间内没有用量也返回零值，最近账号只列出区间内有完成用量的账号
        statement.push(if query.account_ids.is_some() {
            "left join lateral"
        } else {
            "join lateral"
        });
        statement
            .push(" (select mr.started_at from request_analytics_facts mr where mr.provider_account_ref=pa.id and mr.started_at >= ")
            .push_bind(query.range.start)
            .push(" and mr.started_at < ")
            .push_bind(query.range.end);
        push_completed_usage_fact_filter(&mut statement, "mr");
        statement.push(" order by mr.started_at desc,mr.id desc limit 1) latest on true");
        if let Some(account_ids) = &query.account_ids {
            statement
                .push(" where pa.id=any(")
                .push_bind(account_ids.clone())
                .push("::text[])");
        }
        statement
            .push(" order by latest.started_at desc nulls last,pa.name,pa.id limit ")
            .push_bind(i64::from(query.limit));
        let rows = statement
            .build()
            .fetch_all(&mut *self.tx)
            .await
            .map_err(|error| postgres_unavailable("select provider account usage", error))?;
        rows.iter()
            .map(|row| {
                Ok(ProviderAccountUsageObservation {
                    account_id: get(row, "id")?,
                    provider_kind: get(row, "provider_kind")?,
                    authentication_kind: get(row, "authentication_kind")?,
                    name: get(row, "name")?,
                    email: get(row, "email")?,
                    plan_type: get(row, "plan_type")?,
                    request_count: 0,
                    success_count: 0,
                    input_tokens: None,
                    output_tokens: None,
                    cached_input_tokens: None,
                    cache_write_input_tokens: None,
                    reasoning_output_tokens: None,
                    image_input_tokens: None,
                    image_output_tokens: None,
                    image_request_count: 0,
                    image_request_failed_count: 0,
                    total_tokens: None,
                    cost_coverage: CostCoverage::default(),
                    costs: Vec::new(),
                    last_used_at: get(row, "last_used_at")?,
                    request_buckets: Vec::new(),
                    models: Vec::new(),
                })
            })
            .collect()
    }

    /// 与其他统计一样合并互斥的完整桶和剩余窄事实，读取量只随账号模型组合与桶数增长
    async fn read_account_models(
        &mut self,
        range: ObservabilityRange,
        account_ids: &[String],
        mut visit: impl FnMut(String, String, Position, MetricState) -> StoreResult<()>,
    ) -> StoreResult<()> {
        let (hours, quarters) = self.summary_buckets(range, true).await?;
        for (table, seconds, buckets) in [
            ("request_analytics_hourly", 3600, hours),
            ("request_analytics_15m", 900, quarters),
        ] {
            if buckets.is_empty() {
                continue;
            }
            let mut query = QueryBuilder::<Postgres>::new(format!(
                "select bucket_start, dimensions, counts, costs, '{{}}'::jsonb as distributions from {table} a where collection="
            ));
            query
                .push_bind(ACCOUNT_MODEL_COLLECTION)
                .push(" and schema_version=")
                .push_bind(SCHEMA_VERSION)
                .push(" and histogram_version=")
                .push_bind(HISTOGRAM_VERSION);
            push_summary_buckets(&mut query, &buckets, seconds);
            query
                .push(" and dimensions[1]=any(")
                .push_bind(account_ids)
                .push(") limit 100001");
            let mut rows = query.build().fetch(&mut *self.tx);
            let mut count = 0;
            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|error| postgres_unavailable("read account usage summaries", error))?
            {
                count += 1;
                if count > MAX_FACT_ROWS {
                    return Err(unavailable(
                        "account analytics aggregate row budget exceeded",
                    ));
                }
                let mut dimensions = get::<Vec<String>>(&row, "dimensions")?.into_iter();
                let (Some(account), Some(model)) = (dimensions.next(), dimensions.next()) else {
                    return Err(invalid("invalid account usage summary dimensions"));
                };
                let start: DateTime<Utc> = get(&row, "bucket_start")?;
                let end = start + TimeDelta::seconds(seconds);
                visit(
                    account,
                    model,
                    Position::Bucket { start, end },
                    MetricState::decode(&row)?,
                )?;
            }
        }

        let spans = self.uncovered_spans(range).await?;
        if spans.is_empty() {
            return Ok(());
        }
        let (starts, ends): (Vec<_>, Vec<_>) = spans.into_iter().unzip();
        let mut query = QueryBuilder::<Postgres>::new("select fact.* from unnest(");
        query
            .push_bind(starts)
            .push("::timestamptz[],")
            .push_bind(ends)
            .push("::timestamptz[]) span(start_at,end_at) cross join lateral (")
            .push(fact_select())
            .push(" where mr.started_at >= span.start_at and mr.started_at < span.end_at and mr.provider_account_ref=any(")
            .push_bind(account_ids)
            .push(")");
        push_unrecovered_request_filter(&mut query, "mr");
        push_completed_usage_fact_filter(&mut query, "mr");
        query.push(" limit 100001) fact limit 100001");
        let mut rows = query.build().fetch(&mut *self.tx);
        let mut count = 0;
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|error| postgres_unavailable("read account usage facts", error))?
        {
            count += 1;
            if count > MAX_FACT_ROWS {
                return Err(unavailable("account analytics fact row budget exceeded"));
            }
            let Some(mut dimensions) = account_model_dimensions(&row)?.map(Vec::into_iter) else {
                continue;
            };
            let (Some(account), Some(model)) = (dimensions.next(), dimensions.next()) else {
                return Err(invalid("invalid account usage fact dimensions"));
            };
            let mut state = MetricState::default();
            state.add_row(&row)?;
            visit(
                account,
                model,
                Position::Fact(get(&row, "started_at")?),
                state,
            )?;
        }
        Ok(())
    }

    /// 汇总只记录到桶，最近使用时间仅回到各模型最新完整桶内按账号索引取一行
    async fn model_last_used(
        &mut self,
        usage: &BTreeMap<(String, String), ModelUsage>,
    ) -> StoreResult<HashMap<(String, String), DateTime<Utc>>> {
        let mut accounts = Vec::new();
        let mut models = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        for ((account, model), item) in usage {
            // 窄事实与完整桶互斥，事实晚于桶开始时它一定晚于整个桶
            if let Some((start, end)) = item.latest_bucket
                && !model.is_empty()
                && item.latest_fact.is_none_or(|fact| fact < start)
            {
                accounts.push(account.clone());
                models.push(model.clone());
                starts.push(start);
                ends.push(end);
            }
        }
        if accounts.is_empty() {
            return Ok(HashMap::new());
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "select probe.account, probe.model, latest.started_at from unnest(",
        );
        query
            .push_bind(accounts)
            .push("::text[],")
            .push_bind(models)
            .push("::text[],")
            .push_bind(starts)
            .push("::timestamptz[],")
            .push_bind(ends)
            .push("::timestamptz[]) probe(account,model,start_at,end_at) cross join lateral (select mr.started_at from request_analytics_facts mr where mr.provider_account_ref=probe.account and mr.started_at >= probe.start_at and mr.started_at < probe.end_at and coalesce(mr.upstream_model_id,mr.requested_model_id,'')=probe.model");
        push_unrecovered_request_filter(&mut query, "mr");
        push_completed_usage_fact_filter(&mut query, "mr");
        query.push(" order by mr.started_at desc,mr.id desc limit 1) latest");
        let rows = query
            .build()
            .fetch_all(&mut *self.tx)
            .await
            .map_err(|error| postgres_unavailable("read account model last use", error))?;
        rows.iter()
            .map(|row| {
                Ok((
                    (get(row, "account")?, get(row, "model")?),
                    get(row, "started_at")?,
                ))
            })
            .collect()
    }

    /// 每小时请求柱复用同一组账号模型汇总，按 UTC 小时合并完成用量
    async fn account_request_buckets(
        &mut self,
        range: ObservabilityRange,
        account_ids: &[String],
    ) -> StoreResult<HashMap<String, Vec<ProviderAccountRequestBucket>>> {
        let mut counts = HashMap::<String, BTreeMap<i64, u64>>::new();
        self.read_account_models(range, account_ids, |account, _, position, state| {
            let time = match position {
                Position::Bucket { start, .. } => start,
                Position::Fact(started_at) => started_at,
            };
            let index = (bucket_for(time, 3600)? - range.start).num_hours();
            let count = counts.entry(account).or_default().entry(index).or_default();
            *count = count
                .checked_add(state.count("completed_request_count"))
                .ok_or_else(|| invalid("account request timeline overflow"))?;
            Ok(())
        })
        .await?;
        let mut timelines = HashMap::with_capacity(account_ids.len());
        for account_id in account_ids {
            let mut observed = counts.remove(account_id).unwrap_or_default();
            let mut buckets = Vec::new();
            for index in 0..ACCOUNT_USAGE_TIMELINE_HOURS {
                let bucket_start = range
                    .start
                    .checked_add_signed(TimeDelta::hours(index))
                    .ok_or_else(|| {
                        invalid("account request timeline exceeds supported timestamps")
                    })?;
                buckets.push(ProviderAccountRequestBucket {
                    bucket_start,
                    request_count: observed.remove(&index).unwrap_or_default(),
                });
            }
            timelines.insert(account_id.clone(), buckets);
        }
        Ok(timelines)
    }
}
