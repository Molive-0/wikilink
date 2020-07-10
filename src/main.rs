use std::fmt;
use std::collections::{HashSet,HashMap};
use minreq;
use std::hash::{Hasher,Hash};
use serde_json::Value;
use std::sync::{Mutex, Arc};
use std::mem;
use read_input::prelude::*;
use percent_encoding::{utf8_percent_encode, DEFAULT_ENCODE_SET};
use std::fs::File;
use chrono::prelude::*;
use std::io::Write;
use threadpool::ThreadPool;
use std::{thread, time};
use rand::distributions::{Distribution, Uniform};

#[derive(Eq, Debug, Clone)]
struct LinkFollower {
    previous_links: Vec<u32>,
    current_link: u32,
    depth: i32,
    domain: String
}
impl LinkFollower {
    fn from_link(link: &str, domain: &str) -> Result<LinkFollower, &'static str> {
        let id = WikiLinker::to_pageid(link, domain)?;
        Ok(LinkFollower { depth: 0, current_link: id, previous_links: Vec::new(), domain: String::from(domain)}) 
    }
    fn from_pageid(id: u32, domain: &str) -> LinkFollower {
        LinkFollower { depth: 0, current_link: id, previous_links: Vec::new(), domain: String::from(domain)} 
    }
    fn new(id: u32, depth: i32, previous_links: Vec<u32>, domain: &str) -> LinkFollower {
        LinkFollower { depth: depth, current_link: id, previous_links: previous_links, domain: String::from(domain)} 
    }
    #[inline]
    fn increment_link_for_movement(&mut self) {
        self.previous_links.push(self.current_link);
        self.depth += 1;
    }
    fn combine(forward: &LinkFollower, backward: &LinkFollower) -> String {
        assert_eq!(forward.domain, backward.domain);
        let mut links = forward.previous_links.clone();
        links.push(forward.current_link);
        links.extend(&backward.previous_links);
        let mapping = WikiLinker::to_titles(&links, &forward.domain).unwrap();
        let mut out = String::new();
        for i in &forward.previous_links {
            out += format!("{} -> ", mapping[i]).as_str();
        }
        out += format!("{}", mapping[&forward.current_link]).as_str();
        for i in backward.previous_links.iter().rev() {
            out += format!(" -> {}", mapping[i]).as_str();
        }
        out += format!(" at depth {}", forward.depth + backward.depth).as_str();
        out
    }
    fn combine_with_mapping(forward: &LinkFollower, backward: &LinkFollower, mapping: &HashMap<u32, String>) -> String {
        assert_eq!(forward.domain, backward.domain);
        let mut out = String::new();
        for i in &forward.previous_links {
            out += format!("{} -> ", mapping[i]).as_str();
        }
        out += format!("{}", mapping[&forward.current_link]).as_str();
        for i in backward.previous_links.iter().rev() {
            out += format!(" -> {}", mapping[i]).as_str();
        }
        out += format!(" at depth {}", forward.depth + backward.depth).as_str();
        out
    }
}
impl fmt::Display for LinkFollower {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut links = self.previous_links.clone();
        links.push(self.current_link);
        let mapping = WikiLinker::to_titles(&links, &self.domain).unwrap();
        for i in &self.previous_links {
            write!(f, "{} -> ", mapping[i])?;
        }
        write!(f, "{}", mapping[&self.current_link])
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

struct WikiLinker {
    links: Vec<LinkFollower>,
    backlinks: Vec<LinkFollower>,
    fvisited: HashSet<u32>,
    bvisited: HashSet<u32>,
    namespaces: String,
    batch: usize,
}

struct Connection<'a> {
    forward: &'a LinkFollower,
    backward: &'a LinkFollower
}

impl WikiLinker {

    fn new() -> WikiLinker {
        WikiLinker { links: vec![], backlinks: vec![],
            fvisited: HashSet::new(), bvisited: HashSet::new(),
        namespaces: String::from("0"), batch: 50}
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
                        Ok(s) => {return Some(s)}
                    }
                }
                Err(e) => {
                    eprintln!("Page {} failed to retrieve with error {}, ignoring.", uri, e);
                    //return None;
                }
            }
            thread::sleep(time::Duration::from_secs_f64(uniform.sample(&mut rng)));
        }
    }

    fn to_pageid(title: &str, domain: &str) -> Result<u32,&'static str> {
        let uri = format!("https://{1}/api.php?action=query&format=json&redirects=1&titles={0}",
            utf8_percent_encode(title, DEFAULT_ENCODE_SET), domain);
        let p = match WikiLinker::get_content(&uri) {
            Some(s) => {s}
            None => {return Err("No content available")}
        };
        match p["query"]["pages"].as_object().expect("\"pages\" wasn't an object")
        .keys().next().expect("pages conained no values, despite asking for some").as_str(){
            "-1" => {Err("Page does not exist.")}
            s => {Ok(s.parse::<u32>().expect(format!("{} is not a pageid, api gave wrong value?", s).as_str()))}
        }
    }

    fn to_titles(pageids: &Vec<u32>, domain: &str) -> Result<HashMap<u32, String>,&'static str> {
        if pageids.len() > 50 {
            return Err("Too many titles");
        }
        let uri = format!("https://{1}/api.php?action=query&format=json&redirects=1&pageids={0}",
            pageids.into_iter().map(|x| x.to_string()).collect::<Vec<String>>().join("|"), domain);
        let p = match WikiLinker::get_content(&uri) {
            Some(s) => {s}
            None => {return Err("No content available")}
        };
        let mut map: HashMap<u32, String> = HashMap::new();
        for page in p["query"]["pages"].as_object().unwrap().values() {
            map.insert(page["pageid"].as_u64().unwrap() as u32,  page["title"].as_str().unwrap().to_string());
        }
        Ok(map)
    }

    fn check_end(&mut self, domain: String) -> bool {
        println!("Checking pages...");
        let mut connections: HashSet<(&LinkFollower,&LinkFollower)> = HashSet::new();
        //let connections = Arc::new(Mutex::new(connections));
        //let mut tasks = Vec::new();
        //for index in 0..self.links.len() {
        //    let llinks = Arc::clone(&self.links);
        //    let lbacklinks = Arc::clone(&self.backlinks);
        //    let lconnections = Arc::clone(&connections);
        //    tasks.push(thread::spawn(move || {
        //        let mut local = HashSet::new();
        //        for b in lbacklinks.iter() {
        //            if llinks[index] == *b {
        //                local.insert((&llinks[index],b));
        //            }
        //        }
        //        let mut c = lconnections.lock().unwrap();
        //        c.extend(local);
        //    }));
        //}
        //for t in tasks {t.join();}
        //let connections = Arc::try_unwrap(connections).unwrap().into_inner().unwrap();
        for f in self.links.iter() {
            for b in self.backlinks.iter() {
                if f == b {
                    connections.insert((&f,&b));
                }
            }
        }
        if connections.is_empty() {
            return false;
        }
        println!("Connections found, generating title hashmap");
        let mut pageids: HashSet<u32> = HashSet::new();
        for c in &connections {
            pageids.extend(&c.0.previous_links);
            pageids.extend(&c.1.previous_links);
            pageids.insert(c.0.current_link);
        }
        let mapping = Arc::new(Mutex::new(HashMap::new()));
        let pool = ThreadPool::new(self.batch);
        for chunk in pageids.into_iter().collect::<Vec<u32>>().chunks(50) {
            let localdomain = domain.clone();
            let localarcmutex = Arc::clone(&mapping);
            let localchunk = chunk.to_vec();
            pool.execute(move || {
            let map = WikiLinker::to_titles(&localchunk,localdomain.as_str()).unwrap();
            localarcmutex.lock().unwrap().extend(map);});
        }
        pool.join();
        let mapping = Arc::try_unwrap(mapping).unwrap().into_inner().unwrap();
        let now = Utc::now();
        let filename = format!("link_dated_{}-{}-{}_{}-{}-{}.txt",now.year(),now.month(),now.day(),now.hour(),now.minute(),now.second());
        let mut f = File::create(filename).unwrap();
        for c in connections {
            let s = LinkFollower::combine_with_mapping(c.0,c.1,&mapping);
            println!("{}",s);
            writeln!(f,"{}",s).unwrap();
        }
        let count = format!("In the end, there were {} links going forward and {} links going backwards that were added to the graph.", self.links.len()+self.fvisited.len(), self.backlinks.len()+self.bvisited.len());
        println!("{}",count);      
        writeln!(f,"{}",count).unwrap();
        return true;
    }

    fn find_links(link: &LinkFollower, namespaces: &str) -> Option<Vec<u32>> {
        let mut titles = Vec::new();
        let mut uri = format!("https://{2}/api.php?action=query&format=json&pageids={0}&generator=links&gpllimit=max&gplnamespace={1}&redirects=1&indexpageids=1",
            link.current_link, namespaces, link.domain);
        let mut more_items = true;
        while more_items{
            let pagecontent = WikiLinker::get_content(&uri)?;
            more_items = pagecontent.as_object().unwrap().contains_key("continue");
            if more_items {
                uri = format!("https://{4}/api.php?action=query&format=json&pageids={0}&generator=links&gpllimit=max&gplnamespace={1}&redirects=1&indexpageids=1&continue={2}&gplcontinue={3}",
                    link.current_link, namespaces,
                    pagecontent["continue"]["continue"].as_str().unwrap(),
                    utf8_percent_encode(pagecontent["continue"]["gplcontinue"].as_str().unwrap(), DEFAULT_ENCODE_SET),
                    link.domain);
            }
            let newpages = pagecontent["query"]["pageids"].as_array()?;
            let mut newpages = newpages.into_iter().map(|x| x.as_str().unwrap().parse::<i64>()
            .expect(&format!("Value is {}",x.as_str().unwrap().to_string()))).collect::<Vec<i64>>();
            newpages.retain(|&x| x > 0);
            titles.extend(newpages.into_iter().map(|x| x as u32));
        }
        Some(titles)
    }

    fn find_backlinks(link :&LinkFollower, namespaces: &str) -> Option<Vec<u32>> {
        let linkarray = WikiLinker::find_backlinks_inner(link, namespaces, "!redirect");
        let redirects = WikiLinker::get_all_redirects(link, namespaces);
        if redirects.is_none() {
            return linkarray;
        }
        let mut linkarray = linkarray.unwrap_or_default();
        let redirects = redirects.unwrap();
        for a in 0..redirects.len() {
            let linkfollower = LinkFollower::from_pageid(redirects[a], link.domain.as_str());
                match WikiLinker::find_backlinks(&linkfollower, &namespaces) {
                    None => {}
                    Some(s) => {
                        linkarray.extend(s);
                    }
                }
        };
        Some(linkarray)
    }

    fn find_backlinks_inner(link: &LinkFollower, namespaces: &str, redirects: &str) -> Option<Vec<u32>> {
        let mut titles = Vec::new();
        let mut uri = format!("https://{2}/api.php?action=query&format=json&prop=linkshere&pageids={0}&lhprop=pageid&lhlimit=max&lhnamespace={1}&lhshow={3}",
            link.current_link, namespaces, link.domain, redirects);
        let mut more_items = true;
        while more_items{
            let pagecontent = WikiLinker::get_content(&uri)?;
            more_items = pagecontent.as_object().unwrap().contains_key("continue");
            if more_items {
                uri = format!("https://{4}/api.php?action=query&format=json&prop=linkshere&pageids={0}&lhprop=pageid&lhlimit=max&lhnamespace={1}&continue={2}&lhcontinue={3}&lhshow={5}",
                    link.current_link, namespaces,
                    pagecontent["continue"]["continue"].as_str().unwrap(),
                    utf8_percent_encode(pagecontent["continue"]["lhcontinue"].as_str().unwrap(), DEFAULT_ENCODE_SET),
                    link.domain, redirects);
            }
            //println!("{}",pagecontent);
            let newpages = pagecontent["query"]["pages"][link.current_link.to_string()].as_object().unwrap();
            if !newpages.contains_key("linkshere") {return None}
            let newpages = newpages["linkshere"].as_array().unwrap();
            let mut newpages = newpages.into_iter().map(|x| x["pageid"].as_i64().unwrap()).collect::<Vec<i64>>();
            newpages.retain(|&x| x > 0);
            titles.extend(newpages.into_iter().map(|x| x as u32));
        }
        Some(titles)
    }

    fn get_all_redirects(link: &LinkFollower,namespaces: &str) -> Option<Vec<u32>> {
        let mut titles = Vec::new();
        let mut uri = format!("https://{2}/api.php?action=query&format=json&prop=redirects&pageids={0}&rdprop=pageid&rdlimit=max&rdnamespace={1}",
            link.current_link, namespaces, link.domain,);
        let mut more_items = true;
        while more_items{
            let pagecontent = WikiLinker::get_content(&uri)?;
            more_items = pagecontent.as_object().unwrap().contains_key("continue");
            if more_items {
                uri = format!("https://{4}/api.php?action=query&format=json&prop=redirects&pageids={0}&rdprop=pageid&rdlimit=max&rdnamespace={1}&continue={2}&rdcontinue={3}",
                    link.current_link, namespaces,
                    pagecontent["continue"]["continue"].as_str().unwrap(),
                    utf8_percent_encode(pagecontent["continue"]["rdcontinue"].as_str().unwrap(), DEFAULT_ENCODE_SET),
                    link.domain);
            }
            //println!("{}",pagecontent);
            let newpages = pagecontent["query"]["pages"][link.current_link.to_string()].as_object().unwrap();
            if !newpages.contains_key("redirects") {continue}
            let newpages = newpages["redirects"].as_array().unwrap();
            let mut newpages = newpages.into_iter().map(|x| x["pageid"].as_i64().unwrap()).collect::<Vec<i64>>();
            newpages.retain(|&x| x > 0);
            titles.extend(newpages.into_iter().map(|x| x as u32));
        }
        Some(titles)
    }

    fn do_forward_link_pass(&mut self) {
        let pool = ThreadPool::new(self.batch);
        let linkarray = vec![];
        let arcmutex = Arc::new(Mutex::new(linkarray));
        for a in 0..self.links.len() {
            let index = a+1;
            let mut link = self.links[a].clone();
            let length = self.links.len();
            let namespace = self.namespaces.clone();
            let localarcmutex = Arc::clone(&arcmutex);
            pool.execute(move || {
                println!("{} / {} scheduled",index,length);
                match WikiLinker::find_links(&link, &namespace) {
                    None => {println!("{} / {} failed",index,length);}
                    Some(s) => {
                        println!("{} / {} centralizing",index,length);
                        link.increment_link_for_movement();
                        let mut uploadlinks = localarcmutex.lock().unwrap();
                        println!("{} / {} lock achieved",index,length);
                        for l in s {
                            let mut newlink = link.clone();
                            newlink.current_link = l;
                            uploadlinks.push(newlink);
                        }
                        println!("{} / {} complete",index,length);
                    }
                }
            });
        }
        pool.join();
        println!("Retrieving lock...");
        let linkarray = Arc::try_unwrap(arcmutex).expect("Lock is still held somewhere!");
        let mut linkarray = linkarray.into_inner().expect("Mutex not unlocking");
        println!("Removing duplicates...");
        for link in self.links.iter() {self.fvisited.insert(link.current_link);}
        linkarray.retain(|x| !self.fvisited.contains(&x.current_link));
        mem::swap(&mut self.links, &mut linkarray);
    }
    fn do_backward_link_pass(&mut self) {
        let pool = ThreadPool::new(self.batch);
        let linkarray = vec![];
        let arcmutex = Arc::new(Mutex::new(linkarray));
        for a in 0..self.backlinks.len() {
            let index = a+1;
            let mut link = self.backlinks[a].clone();
            let length = self.backlinks.len();
            let namespace = self.namespaces.clone();
            let localarcmutex = Arc::clone(&arcmutex);
            pool.execute(move || {
                println!("{} \\ {} scheduled",index,length);
                match WikiLinker::find_backlinks(&link, &namespace) {
                    None => {println!("{} \\ {} failed",index,length);}
                    Some(s) => {
                        println!("{} \\ {} centralizing",index,length);
                        link.increment_link_for_movement();
                        let mut uploadlinks = localarcmutex.lock().unwrap();
                        println!("{} \\ {} lock achieved",index,length);
                        for l in s {
                            let mut newlink = link.clone();
                            newlink.current_link = l;
                            uploadlinks.push(newlink);
                        }
                        println!("{} \\ {} complete",index,length);
                    }
                }
            });
        }
        pool.join();
        println!("Retrieving lock...");
        let linkarray = Arc::try_unwrap(arcmutex).expect("Lock is still held somewhere!");
        let mut linkarray = linkarray.into_inner().expect("Mutex not unlocking");
        println!("Removing duplicates...");
        for link in self.backlinks.iter() {self.bvisited.insert(link.current_link);}
        linkarray.retain(|x| !self.bvisited.contains(&x.current_link));
        mem::swap(&mut self.backlinks, &mut linkarray);
    }

    fn perform_search(&mut self, start: &str, end: &str, domain: &str) {
        self.links.push(LinkFollower::from_link(start, domain).unwrap());
        self.backlinks.push(LinkFollower::from_link(end, domain).unwrap());
        if self.check_end(String::from(domain)) {
            return;
        }
        loop {
            while self.backlinks.len() <= self.links.len() {
                self.do_backward_link_pass();
                if self.check_end(String::from(domain)) {
                    return;
                }
                if self.backlinks.len() == 0 {
                    eprintln!("No article has a link to {}", end);
                    return;
                }
            }
            self.do_forward_link_pass();
            if self.check_end(String::from(domain)) {
                return;
            }
            if self.links.len() == 0 {
                eprintln!("{} is a dead end", start);
                return;
            }
        }
    }
}

fn main() {
    let mut wl = WikiLinker::new();
    let mut domain = input::<String>()
    .msg("Enter a domain (default is en.wikipedia.org/w/): ")
    .default(String::from("en.wikipedia.org/w")).get();
    if domain.ends_with("/") {domain.pop();} let domain = domain;
    let firstlink = input::<String>().default(String::from("Tacoma Narrows Bridge")).msg("Enter a starting page (eg. Tacoma Narrows Bridge): ").get();
    let endlink = input::<String>().default(String::from("24-Hour Analog Dial")).msg("Enter an ending page (eg. 24-Hour Analog Dial): ").get();
    let extra_namespace = input::<String>().msg("Allow extended namespaces? (if unsure type \"y\") (y\\n): ").get();
    let batch = input::<usize>().repeat_msg("Threads to batch? (Max (and default) is 30, must be greater than 0): ")
    .inside(1..=30).default(30).get();
    wl.namespaces = String::from(if extra_namespace.to_lowercase() == "n" {"0"} else {"0|14|100"});
    wl.batch = batch;
    wl.perform_search(&firstlink, &endlink, &domain);
}
