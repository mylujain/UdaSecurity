package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final List<Pattern> ignorUrl;
  private final Clock clock;
  private final Duration timeout;
  private final PageParserFactory parserFactories;
  private final int popularWordCount;
  private final int threadCount;
  private final ForkJoinPool pool;





  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout, @PopularWordCount int popularWordCount, @TargetParallelism int threadCount,
          @IgnoredUrls List<Pattern> ignoredUrls, @MaxDepth int maxDepth, PageParserFactory parserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.parserFactories = parserFactory;
    this.threadCount = maxDepth;
    this.ignorUrl = ignoredUrls;
  }
public   Map<String, Integer> counts(){

  return Collections.synchronizedMap(new HashMap<>());
}
  public   Set<String> linkVisited(){
    Set<String> urlVisited = Collections.synchronizedSet(new HashSet<>());

    return urlVisited;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {

    Map<String, Integer> counters = this.counts();
    Set<String> linkVisited = this.linkVisited();
    for (String link : startingUrls) {
      pool.invoke(new parallelCrawl(link, clock.instant().plus(timeout), threadCount, counters, linkVisited, clock, parserFactories, ignorUrl));
    }
    int s =linkVisited.size();
    CrawlResult CrawlResult;
    if (counters.isEmpty()) {
       CrawlResult=new CrawlResult.Builder()
              .setWordCounts(counters)
              .setUrlsVisited(s)
              .build();
      return CrawlResult;
    }

    Map<String, Integer> sorted =WordCounts.sort(counters, popularWordCount);
    CrawlResult= new CrawlResult.Builder()
            .setWordCounts(sorted)
            .setUrlsVisited(s)
            .build();
    return CrawlResult;
  }
  public static class parallelCrawl extends RecursiveTask<Boolean> {
    private final String url;
    private final Map<String, Integer> counts;

    private final int threadCount;

    private final Set<String> visitedUrls;
    private final Clock clock;
    private final Instant deadline;
    private final PageParserFactory parserFactory;
    private final List<Pattern> ignorUrl;




    public parallelCrawl(String url, Instant deadline, int maxDepth, Map<String, Integer> counts, Set<String> visitedUrls, Clock clock,
                         PageParserFactory parserFactory, List<Pattern> ignoredUrls) {
      this.url = url;
      this.counts = counts;
      this.deadline = deadline;
      this.threadCount = maxDepth;
      this.visitedUrls = visitedUrls;
      this.ignorUrl = ignoredUrls;
      this.clock = clock;
      this.parserFactory = parserFactory;

    }

    @Override
    protected Boolean compute() {
      boolean noThread=threadCount == 0;
      boolean after=clock.instant().isAfter(deadline);
      if ( noThread || after) {
        return false;
      }
      boolean match;
      for (Pattern p : ignorUrl) {
        match=p.matcher(url).matches();
        if (match) {
          return false;
        }
      }

      if (!visitedUrls.add(url)) {
        return false;
      }
      List<parallelCrawl> subtasks;
      PageParser.Result result = parserFactory.get(url).parse();
      Set<Map.Entry<String, Integer>> set=result.getWordCounts().entrySet();
      for (ConcurrentMap.Entry<String, Integer> ent : set ) {
        counts.compute(ent.getKey(), (key, value) -> (value == null) ? ent.getValue() : ent.getValue()+value);
      }

       subtasks = new ArrayList<>();
      List<String> links=result.getLinks();
      for (String link :links ) {
        subtasks.add(new parallelCrawl(link, deadline, threadCount - 1, counts, visitedUrls, clock, parserFactory, ignorUrl));
      }
      invokeAll(subtasks);
      return true;
    }

  }
  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
