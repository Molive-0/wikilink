use chrono::prelude::*;
use percent_encoding::{utf8_percent_encode, DEFAULT_ENCODE_SET};
use rand::distributions::{Distribution, Uniform};
use rayon::prelude::*;
use read_input::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::env::args;
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::Arc;
use std::{thread, time};

//#[derive(PartialEq, Eq, Debug, Clone)]
type LinkPointer = Option<Arc<LinkFollower>>;

#[derive(Eq, Debug, Clone)]
struct LinkFollower {
    previous_links: LinkPointer,
    current_link: u32,
}
impl LinkFollower {
    fn new(id: u32, previous_links: LinkPointer) -> LinkFollower {
        LinkFollower {
            current_link: id,
            previous_links,
        }
    }
    fn get_links(&self) -> Vec<u32> {
        let mut links = Vec::new();
        let mut pointer = &self.previous_links;
        loop {
            match pointer {
                Some(l) => {
                    links.push(l.current_link);
                    pointer = &l.previous_links;
                }
                None => {
                    return links;
                }
            }
        }
    }
    fn get_depth(&self) -> u32 {
        let mut depth = 0;
        let mut pointer = &self.previous_links;
        loop {
            match pointer {
                Some(l) => {
                    depth += 1;
                    pointer = &l.previous_links;
                }
                None => {
                    return depth;
                }
            }
        }
    }
    // #[inline]
    // fn increment_link_for_movement(&mut self) {
    //     self.previous_links.push(self.current_link);
    //     self.depth += 1;
    // }
}
impl fmt::Display for LinkFollower {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for i in self.get_links().iter().rev() {
            write!(f, "{} -> ", i)?;
        }
        write!(f, "{}", &self.current_link)
    }
}
impl PartialEq for LinkFollower {
    fn eq(&self, other: &Self) -> bool {
        self.current_link == other.current_link
    }
}
impl Hash for LinkFollower {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.current_link.hash(state);
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
struct WikiLinker<'a> {
    links: Vec<Arc<LinkFollower>>,
    backlinks: Vec<Arc<LinkFollower>>,
    namespaces: &'a str,
    domain: &'a str,
}

impl<'linker> WikiLinker<'linker> {
    fn follower_from_link(&self, link: &str) -> Result<LinkFollower, &'static str> {
        let id = self.to_pageid(link)?;
        Ok(LinkFollower {
            current_link: id,
            previous_links: None,
        })
    }
    // fn combine(&self, forward: &LinkFollower, backward: &LinkFollower) -> String {
    //     let mut links = forward.get_links();
    //     links.push(forward.current_link);
    //     links.extend(&backward.get_links());
    //     let mapping = self.to_titles(&links).unwrap();
    //     self.combine_with_mapping(forward, backward, &mapping)
    // }
    fn combine_with_mapping(
        &self,
        forward: &LinkFollower,
        backward: &LinkFollower,
        mapping: &HashMap<u32, String>,
    ) -> String {
        let mut out = String::new();
        for i in forward.get_links().iter().rev() {
            out += format!("{} -> ", mapping[i]).as_str();
        }
        out += mapping[&forward.current_link].as_str();
        for i in &backward.get_links() {
            out += format!(" -> {}", mapping[i]).as_str();
        }
        out += format!(" at depth {}", forward.get_depth() + backward.get_depth()).as_str();
        out
    }

    fn new() -> Self {
        WikiLinker {
            links: vec![],
            backlinks: vec![],
            namespaces: "0|14|100",
            domain: "en.wikipedia.org/w",
        }
    }

    fn get_content(uri: &str) -> Option<Value> {
        let mut rng = rand::thread_rng();
        let uniform = Uniform::from(0.5..2.0);
        loop {
            match minreq::get(uri).send() {
                Ok(s) => {
                    match s.json::<Value>() {
                        Err(e) => {
                            eprintln!("Running repeat on thread, {}", e);
                            //eprintln!("Website returned invalid JSON, sure why not. Uri: {} Error: {}", uri, e);
                            //eprintln!("JSON was {}", s.as_str().unwrap());
                        }
                        Ok(s) => return Some(s),
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Page {} failed to retrieve with error {}, ignoring.",
                        uri, e
                    );
                    //return None;
                }
            }
            thread::sleep(time::Duration::from_secs_f64(uniform.sample(&mut rng)));
        }
    }

    fn to_pageid(&self, title: &str) -> Result<u32, &'static str> {
        let uri = format!(
            "https://{1}/api.php?action=query&format=json&redirects=1&titles={0}",
            utf8_percent_encode(title, DEFAULT_ENCODE_SET),
            self.domain
        );
        let p = match WikiLinker::get_content(&uri) {
            Some(s) => s,
            None => return Err("No content available"),
        };
        match p["query"]["pages"]
            .as_object()
            .expect("\"pages\" wasn't an object")
            .keys()
            .next()
            .expect("pages conained no values, despite asking for some")
            .as_str()
        {
            "-1" => Err("Page does not exist."),
            s => Ok(s
                .parse::<u32>()
                .unwrap_or_else(|_| panic!("{} is not a pageid, api gave wrong value?", s))),
        }
    }

    fn to_titles(&self, pageids: &[u32]) -> Result<HashMap<u32, String>, &'static str> {
        if pageids.len() > 50 {
            return Err("Too many titles");
        }
        let uri = format!(
            "https://{1}/api.php?action=query&format=json&redirects=1&pageids={0}",
            pageids
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join("|"),
            self.domain
        );
        let p = match WikiLinker::get_content(&uri) {
            Some(s) => s,
            None => return Err("No content available"),
        };
        let mut map: HashMap<u32, String> = HashMap::new();
        for page in p["query"]["pages"].as_object().unwrap().values() {
            map.insert(
                page["pageid"].as_u64().unwrap() as u32,
                page["title"].as_str().unwrap().to_string(),
            );
        }
        Ok(map)
    }

    fn check_end(&mut self) -> bool {
        println!("Checking pages...");
        let mut connections: HashSet<(Arc<LinkFollower>, Arc<LinkFollower>)> = HashSet::new();
        connections.par_extend(
            self.links
                .par_iter()
                .flat_map(|f| {
                    self.backlinks
                        .par_iter()
                        .map(move |b| (f.clone(), b.clone()))
                })
                .filter(|(f, b)| f == b),
        );
        if connections.is_empty() {
            return false;
        }
        println!("Connections found, generating title hashmap");
        let mut pageids: HashSet<u32> = HashSet::new();
        for c in &connections {
            pageids.extend(c.0.get_links());
            pageids.extend(c.1.get_links());
            pageids.insert(c.0.current_link);
        }
        let mapping = pageids // this is a mess and i love it
            .into_iter()
            .collect::<Vec<u32>>()
            .chunks(50)
            .collect::<Vec<&[u32]>>()
            .par_iter()
            .map(|chunk| self.to_titles(&chunk).unwrap())
            .flatten()
            .collect();
        let now = Utc::now();
        let filename = format!(
            "link_dated_{}-{}-{}_{}-{}-{}.txt",
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            now.minute(),
            now.second()
        );
        let mut f = File::create(filename).unwrap();
        for c in connections {
            let s = self.combine_with_mapping(&c.0, &c.1, &mapping);
            println!("{}", s);
            writeln!(f, "{}", s).unwrap();
        }
        let count = format!("In the end, there were {} links going forward and {} links going backwards that were added to the graph.", self.links.len(), self.backlinks.len());
        println!("{}", count);
        writeln!(f, "{}", count).unwrap();
        true
    }

    fn find_links(&self, link: &LinkFollower) -> Option<Vec<u32>> {
        let mut titles = Vec::new();
        let mut uri = format!("https://{2}/api.php?action=query&format=json&pageids={0}&generator=links&gpllimit=max&gplnamespace={1}&redirects=1&indexpageids=1",
            link.current_link, self.namespaces, self.domain);
        let mut more_items = true;
        while more_items {
            let pagecontent = WikiLinker::get_content(&uri)?;
            more_items = pagecontent.as_object().unwrap().contains_key("continue");
            if more_items {
                uri = format!("https://{4}/api.php?action=query&format=json&pageids={0}&generator=links&gpllimit=max&gplnamespace={1}&redirects=1&indexpageids=1&continue={2}&gplcontinue={3}",
                    link.current_link, self.namespaces,
                    pagecontent["continue"]["continue"].as_str().unwrap(),
                    utf8_percent_encode(pagecontent["continue"]["gplcontinue"].as_str().unwrap(), DEFAULT_ENCODE_SET),
                    self.domain);
            }
            let newpages = pagecontent["query"]["pageids"].as_array()?;
            let mut newpages = newpages
                .iter()
                .map(|x| {
                    x.as_str()
                        .unwrap()
                        .parse::<i64>()
                        .unwrap_or_else(|_| panic!("Value is {}", x.as_str().unwrap()))
                })
                .collect::<Vec<i64>>();
            newpages.retain(|&x| x > 0);
            titles.extend(newpages.iter().map(|x| *x as u32));
        }
        Some(titles)
    }

    fn find_backlinks(&self, link: &LinkFollower) -> Option<Vec<u32>> {
        let linkarray = self.find_backlinks_inner(link, "!redirect");
        let redirects = self.get_all_redirects(link);
        if redirects.is_none() {
            return linkarray;
        }
        let mut linkarray = linkarray.unwrap_or_default();
        let redirects = redirects.unwrap();
        for r in redirects {
            let linkfollower = LinkFollower::new(r, None);
            match self.find_backlinks(&linkfollower) {
                None => {}
                Some(s) => {
                    linkarray.extend(s);
                }
            }
        }
        Some(linkarray)
    }

    fn find_backlinks_inner(&self, link: &LinkFollower, redirects: &str) -> Option<Vec<u32>> {
        let mut titles = Vec::new();
        let mut uri = format!("https://{2}/api.php?action=query&format=json&prop=linkshere&pageids={0}&lhprop=pageid&lhlimit=max&lhnamespace={1}&lhshow={3}",
            link.current_link, self.namespaces, self.domain, redirects);
        let mut more_items = true;
        while more_items {
            let pagecontent = WikiLinker::get_content(&uri)?;
            more_items = pagecontent.as_object().unwrap().contains_key("continue");
            if more_items {
                uri = format!("https://{4}/api.php?action=query&format=json&prop=linkshere&pageids={0}&lhprop=pageid&lhlimit=max&lhnamespace={1}&continue={2}&lhcontinue={3}&lhshow={5}",
                    link.current_link, self.namespaces,
                    pagecontent["continue"]["continue"].as_str().unwrap(),
                    utf8_percent_encode(pagecontent["continue"]["lhcontinue"].as_str().unwrap(), DEFAULT_ENCODE_SET),
                    self.domain, redirects);
            }
            //println!("{}",pagecontent);
            let newpages = pagecontent["query"]["pages"][link.current_link.to_string()]
                .as_object()
                .unwrap();
            if !newpages.contains_key("linkshere") {
                return None;
            }
            let newpages = newpages["linkshere"].as_array().unwrap();
            let mut newpages = newpages
                .iter()
                .map(|x| x["pageid"].as_i64().unwrap())
                .collect::<Vec<i64>>();
            newpages.retain(|&x| x > 0);
            titles.extend(newpages.iter().map(|x| *x as u32));
        }
        Some(titles)
    }

    fn get_all_redirects(&self, link: &LinkFollower) -> Option<Vec<u32>> {
        let mut titles = Vec::new();
        let mut uri = format!("https://{2}/api.php?action=query&format=json&prop=redirects&pageids={0}&rdprop=pageid&rdlimit=max&rdnamespace={1}",
            link.current_link, self.namespaces, self.domain);
        let mut more_items = true;
        while more_items {
            let pagecontent = WikiLinker::get_content(&uri)?;
            more_items = pagecontent.as_object().unwrap().contains_key("continue");
            if more_items {
                uri = format!("https://{4}/api.php?action=query&format=json&prop=redirects&pageids={0}&rdprop=pageid&rdlimit=max&rdnamespace={1}&continue={2}&rdcontinue={3}",
                    link.current_link, self.namespaces,
                    pagecontent["continue"]["continue"].as_str().unwrap(),
                    utf8_percent_encode(pagecontent["continue"]["rdcontinue"].as_str().unwrap(), DEFAULT_ENCODE_SET),
                    self.domain);
            }
            //println!("{}",pagecontent);
            let newpages = pagecontent["query"]["pages"][link.current_link.to_string()]
                .as_object()
                .unwrap();
            if !newpages.contains_key("redirects") {
                continue;
            }
            let newpages = newpages["redirects"].as_array().unwrap();
            let mut newpages = newpages
                .iter()
                .map(|x| x["pageid"].as_i64().unwrap())
                .collect::<Vec<i64>>();
            newpages.retain(|&x| x > 0);
            titles.extend(newpages.iter().map(|x| *x as u32));
        }
        Some(titles)
    }

    fn do_forward_link_pass(&mut self) {
        let length = self.links.len();
        let mut linkarray: Vec<Arc<LinkFollower>> = self
            .links
            .par_iter()
            .enumerate()
            .map(|(a, link)| {
                let index = a + 1;
                println!("{} / {} scheduled", index, length);
                match self.find_links(&link) {
                    None => {
                        println!("{} / {} failed", index, length);
                        vec![]
                    }
                    Some(s) => {
                        println!("{} / {} complete", index, length);
                        s.iter()
                            .map(|item| Arc::new(LinkFollower::new(*item, Some(link.clone()))))
                            .collect::<Vec<Arc<LinkFollower>>>()
                    }
                }
            })
            .flatten()
            .collect();
        println!("Removing duplicates...");
        linkarray.retain(|x| !self.links.contains(&x));
        std::mem::swap(&mut self.links, &mut linkarray);
    }
    fn do_backward_link_pass(&mut self) {
        let length = self.backlinks.len();
        let mut linkarray: Vec<Arc<LinkFollower>> = self
            .backlinks
            .par_iter()
            .enumerate()
            .map(|(a, link)| {
                let index = a + 1;
                println!("{} \\ {} scheduled", index, length);
                match self.find_backlinks(&link) {
                    None => {
                        println!("{} \\ {} failed", index, length);
                        vec![]
                    }
                    Some(s) => {
                        println!("{} \\ {} complete", index, length);
                        s.iter()
                            .map(|item| Arc::new(LinkFollower::new(*item, Some(link.clone()))))
                            .collect::<Vec<Arc<LinkFollower>>>()
                    }
                }
            })
            .flatten()
            .collect();
        println!("Removing duplicates...");
        linkarray.retain(|x| !self.backlinks.contains(&x));
        std::mem::swap(&mut self.backlinks, &mut linkarray);
    }

    fn perform_search(&mut self, start: &str, end: &str) {
        self.links
            .push(Arc::new(self.follower_from_link(start).unwrap()));
        self.backlinks
            .push(Arc::new(self.follower_from_link(end).unwrap()));
        if self.check_end() {
            return;
        }
        loop {
            while self.backlinks.len() <= self.links.len() {
                self.do_backward_link_pass();
                if self.check_end() {
                    return;
                }
                if self.backlinks.is_empty() {
                    eprintln!("No article has a link to {}", end);
                    return;
                }
            }
            self.do_forward_link_pass();
            if self.check_end() {
                return;
            }
            if self.links.is_empty() {
                eprintln!("{} is a dead end", start);
                return;
            }
        }
    }
}

fn main() {
    let args: Vec<String> = args().collect();
    if args.len() > 1 && args[1] == "-d" {
        let mut wl = WikiLinker::new();
        rayon::ThreadPoolBuilder::new()
            .num_threads(30)
            .build_global()
            .unwrap();
        wl.perform_search("Tacoma Narrows Bridge", "24-Hour Analog Dial");
    } else {
        let mut wl = WikiLinker::new();
        let mut domain = input::<String>()
            .msg("Enter a domain (default is en.wikipedia.org/w/): ")
            .default(String::from("en.wikipedia.org/w"))
            .get();
        if domain.ends_with('/') {
            domain.pop();
        }
        let domain = domain;
        let firstlink = input::<String>()
            .default(String::from("Tacoma Narrows Bridge"))
            .msg("Enter a starting page (eg. Tacoma Narrows Bridge): ")
            .get();
        let endlink = input::<String>()
            .default(String::from("24-Hour Analog Dial"))
            .msg("Enter an ending page (eg. 24-Hour Analog Dial): ")
            .get();
        let extra_namespace = input::<String>()
            .msg("Allow extended namespaces? (if unsure type \"y\") (y\\n): ")
            .get();
        let batch = input::<usize>()
            .repeat_msg("Threads to batch? (Max (and default) is 30, must be greater than 0): ")
            .inside(1..=30)
            .default(30)
            .get();
        wl.namespaces = if extra_namespace.to_lowercase() == "n" {
            "0"
        } else {
            "0|14|100"
        };
        rayon::ThreadPoolBuilder::new()
            .num_threads(batch)
            .build_global()
            .unwrap();
        wl.domain = domain.as_str();
        wl.perform_search(&firstlink, &endlink);
        let _ = input::<String>().get();
        println!("exit");
    }
}
