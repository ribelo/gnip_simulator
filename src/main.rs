use std::hash::Hash;

use chrono::{DateTime, Duration, Utc};
use indicatif::ProgressBar;
use itertools::Itertools;
use rand::{distributions::Distribution, seq::SliceRandom, Rng};
use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
use rustc_hash::FxHashMap as HashMap;
use statrs::distribution::{LogNormal, Normal, Pareto};

#[derive(Debug, Clone, Copy, Eq)]
struct User {
    id: usize,
    tweet_count: usize,
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for User {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug, Clone)]
struct Tweet {
    user_id: usize,
    timestamp: DateTime<Utc>,
}

fn generate_users(num_users: usize, shape: f64) -> Vec<User> {
    let mut rng = rand::thread_rng();
    let scale = 10.0;
    // let dist = Pareto::new(scale, shape).unwrap();
    // let dist = Normal::new(22.0, 200.0).unwrap();
    let dist = LogNormal::new(0.0, 2.0).unwrap();

    let mut users = Vec::with_capacity(num_users);

    for id in 0..num_users {
        let tweet_count = dist.sample(&mut rng).round().max(0.0) as usize;

        users.push(User { id, tweet_count });
    }

    users
}

fn generate_data(
    num_users: usize,
    shape: f64,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> (Vec<User>, HashMap<usize, Vec<Tweet>>) {
    let mut rng = rand::thread_rng();
    let mut users = generate_users(num_users, shape);
    let mut user_tweets = HashMap::default();

    for user in users.iter_mut() {
        for _ in 0..user.tweet_count {
            let time_offset_secs = rng.gen_range(0..=(end_time - start_time).num_seconds());
            let tweet_time = start_time + Duration::seconds(time_offset_secs);
            let tweet = Tweet {
                user_id: user.id,
                timestamp: tweet_time,
            };
            user_tweets
                .entry(user.id)
                .or_insert_with(Vec::new)
                .push(tweet);
        }
    }

    (users, user_tweets)
}

#[derive(Debug, Default)]
struct ScenarioStats {
    num_requests: usize,
    num_users: usize,
    num_tweets: usize,
    avg_tweets_per_user: usize,
    avg_tweets_per_req: usize,
}

impl ScenarioStats {
    fn update(&mut self, reqs: usize, data: HashMap<usize, usize>) {
        self.num_requests += reqs;
        self.num_users += data.len();
        self.num_tweets += data.values().sum::<usize>();
        self.avg_tweets_per_user = self.num_tweets / self.num_users;
        self.avg_tweets_per_req = self.num_tweets / self.num_requests;
    }
}

#[derive(Debug, Default)]
struct SimulationStats {
    n: usize,
    total_tweets: usize,
    total_users: usize,
    avg_tweets: usize,
    total_requests: usize,
    avg_requests: usize,
    avg_tweets_per_user: usize,
    avg_tweets_per_req: usize,
}

impl SimulationStats {
    fn add_scenario(&self, scenario: ScenarioStats) -> Self {
        let n = self.n + 1;
        let total_tweets = self.total_tweets + scenario.num_tweets;
        let total_users = self.total_users + scenario.num_users;
        let avg_tweets = self.total_tweets / n;
        let total_requests = self.total_requests + scenario.num_requests;
        let avg_requests = total_requests / n;
        let avg_tweets_per_user = total_tweets / total_users;
        let avg_tweets_per_req = total_tweets / total_requests;
        Self {
            n,
            total_tweets,
            total_users,
            avg_tweets,
            total_requests,
            avg_requests,
            avg_tweets_per_user,
            avg_tweets_per_req,
        }
    }
}

impl std::ops::Add for SimulationStats {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        let n = self.n + other.n;
        let total_tweets = self.total_tweets + other.total_tweets;
        let total_users = self.total_users + other.total_users;
        let avg_tweets = total_tweets / n;
        let total_requests = self.total_requests + other.total_requests;
        let avg_requests = total_requests / n;
        let avg_tweets_per_user = total_tweets / total_users;
        let avg_tweets_per_req = total_tweets / total_requests;
        Self {
            n,
            total_tweets,
            total_users,
            avg_tweets,
            total_requests,
            avg_requests,
            avg_tweets_per_user,
            avg_tweets_per_req,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FetchMode {
    First,
    All,
}

fn simulate_request(
    users: &[User],
    users_tweets: &HashMap<usize, Vec<Tweet>>,
    max_tweets_per_request: usize,
    min_tweets_per_user: usize,
    mode: FetchMode,
) -> (usize, HashMap<usize, usize>) {
    let mut result = HashMap::default();
    let mut tweets = Vec::with_capacity(users.len());

    for &user in users {
        result.insert(user.id, 0);
        if users_tweets.contains_key(&user.id) {
            tweets.extend(users_tweets[&user.id].iter());
        }
    }

    tweets.sort_by_key(|tweet| tweet.timestamp);

    let mut req_count = 1;
    let mut idx = 0;
    let mut batch_count = 0;

    // dbg!(tweets.len());
    for tweet in tweets {
        idx += 1;
        batch_count += 1;

        result.entry(tweet.user_id).and_modify(|i| *i += 1);
        if batch_count % max_tweets_per_request == 0 {
            let max_user_tweets = *result.values().max().unwrap();
            let min_user_tweets = *result.values().min().unwrap();
            match mode {
                FetchMode::First => {
                    if max_user_tweets >= min_tweets_per_user {
                        break;
                    } else {
                        req_count += 1;
                    }
                }
                FetchMode::All => {
                    if min_user_tweets >= min_tweets_per_user {
                        break;
                    } else {
                        req_count += 1;
                    }
                }
            }
        }
    }
    // dbg!(idx, batch_count, req_count);
    // dbg!(result.values().max());
    // dbg!(result.values().min());
    (req_count, result)
}

enum UserSortMode {
    Asc,
    Desc,
    Random,
}

fn simulate_scenario(
    users: &[User],
    users_tweets: &HashMap<usize, Vec<Tweet>>,
    max_tweets_per_request: usize,
    max_requests: usize,
    min_tweets_per_user: usize,
    fetch_mode: FetchMode,
) -> ScenarioStats {
    let mut stats = ScenarioStats::default();
    for users_chunk in users.chunks(100) {
        let (reqs, data) = simulate_request(
            users_chunk,
            users_tweets,
            max_tweets_per_request,
            min_tweets_per_user,
            fetch_mode,
        );
        stats.update(reqs, data);
        if stats.num_requests >= max_requests {
            break;
        }
    }

    stats
}

fn main() {
    let num_users = 10_000_000;
    let start_time = Utc::now() - Duration::days(7);
    let end_time = Utc::now();
    let max_requests = 12_500;

    let (mut users, tweets) = generate_data(num_users, 1.5, start_time, end_time);
    println!(
        "total tweets: {}",
        tweets.values().map(|v| v.len()).sum::<usize>()
    );

    let fetch_mode = FetchMode::All;
    println!("asc all");
    users.sort_by_key(|user| user.tweet_count);
    let scenario = simulate_scenario(&users, &tweets, 500, max_requests, 100, fetch_mode);
    dbg!(scenario);
    println!("====");

    // desc
    println!("desc all");
    users.reverse();
    let scenario = simulate_scenario(&users, &tweets, 500, max_requests, 100, fetch_mode);
    dbg!(scenario);
    println!("====");

    println!("random all");
    users.shuffle(&mut rand::thread_rng());
    let scenario = simulate_scenario(&users, &tweets, 500, max_requests, 100, fetch_mode);
    dbg!(scenario);
    println!("====");

    let fetch_mode = FetchMode::First;
    println!("asc first");
    users.sort_by_key(|user| user.tweet_count);
    let scenario = simulate_scenario(&users, &tweets, 500, max_requests, 100, fetch_mode);
    dbg!(scenario);
    println!("====");

    println!("desc first");
    users.reverse();
    let scenario = simulate_scenario(&users, &tweets, 500, max_requests, 100, fetch_mode);
    dbg!(scenario);
    println!("====");

    println!("random first");
    users.shuffle(&mut rand::thread_rng());
    let scenario = simulate_scenario(&users, &tweets, 500, max_requests, 100, fetch_mode);
    dbg!(scenario);
    println!("====");

    // let num_simulations = 100;
    // let bar = ProgressBar::new(num_simulations);
    // let stats = (0..num_simulations)
    //     .into_par_iter()
    //     .map(|_| {
    //         let (mut users, tweets) = generate_data(num_users, 2.0, start_time, end_time);
    //         match sort_mode {
    //             UserSortMode::Asc => {
    //                 users.sort_by_key(|user| user.tweet_count);
    //             }
    //             UserSortMode::Desc => {
    //                 users.sort_by_key(|user| user.tweet_count);
    //                 users.reverse();
    //             }
    //             UserSortMode::Random => {
    //                 users.shuffle(&mut rand::thread_rng());
    //             }
    //         }
    //         let scenario = simulate_scenario(&users, &tweets, 500, 50000, 100, fetch_mode);
    //         bar.inc(1);
    //         scenario
    //     })
    //     .fold(SimulationStats::default, |acc, stats| {
    //         acc.add_scenario(stats)
    //     })
    //     .reduce(SimulationStats::default, |acc, stats| acc + stats);
    // bar.finish();
    // dbg!(stats);
}
