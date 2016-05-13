package univ.bigdata.course;

import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.providers.MoviesProvider;

import java.util.*;
import java.util.Map.Entry;

/*
Nir Rosen; nirosen99@gmail.coml; 311450704
Ran Bakalo;ranbak@gmail.com; 305286197
Nadav Eichler; Nadaveichler@gmail.com; 308027325;
Zhai Yona; Zahiyona2006@gmail.com
Oren Efraimov;Orenef11@gmail.com; 305781544
*/

/**
 * Main class which capable to keep all information regarding movies review.
 * Has to implements all methods from @{@link IMoviesStorage} interface.
 * Also presents functionality to answer different user queries, such as:
 * <p>
 * 2. Total number of distinct users that produces the review.
 * 3. Average review score for all movies.
 * 4. Average review score per single movie.
 * 5. Most popular movie reviewed by at least "K" unique users
 * 6. Word count for movie review, select top "K" words
 * 7. K most helpful users
 */
public class MoviesStorage implements IMoviesStorage {

    private Collection<MovieReview> _movieReviews;

    public MoviesStorage(final MoviesProvider provider) {
        _movieReviews = new ArrayList<MovieReview>();
        // read movies using provider interface
        while (provider.hasMovie()) {
            MovieReview movieReview = provider.getMovie();
            _movieReviews.add(movieReview);
        }
    }

    private MoviesProvider _moviesProvider;

    @Override
    public double totalMoviesAverageScore() {
        double averageScore = 0;
        //Sum score of all reviews movies
        for (MovieReview item : this._movieReviews)
            averageScore += item.getMovie().getScore();

        return Math.round((averageScore / this._movieReviews.size()) * 100d) / 100d;
    }

    @Override
    public List<Movie> getTopKMoviesAverage(long topK) {
        //HashTable which counts the number of reviews per movie
        Hashtable<String, Integer> hashCount = new Hashtable<String, Integer>();
        //HashTable which sum the scores in reviews per movie
        Hashtable<String, Double> hashScore = new Hashtable<String, Double>();

        //Count all reviews
        int sizeMovies = this._movieReviews.size();
        String[] arrMovieName = new String[sizeMovies];
        Movie movie;

        int sizeArrMovieName = 0;
        for (MovieReview item : this._movieReviews) {
            movie = item.getMovie();
            // Checking whether there is a movie's name exists
            if (hashCount.containsKey(movie.getProductId())) {
                hashCount.put(movie.getProductId(), hashCount.get(movie.getProductId()) + 1);
                hashScore.put(movie.getProductId(), hashScore.get(movie.getProductId()) + movie.getScore());
            } else {
                //Save the name movie, because it is a new movie
                arrMovieName[sizeArrMovieName++] = movie.getProductId();
                //Create new filed in hashtable
                hashCount.put(movie.getProductId(), 1);
                hashScore.put(movie.getProductId(), movie.getScore());
            }
        }

        //Sort all data have in two hashtable by score
        List<Movie> listMovies = new ArrayList<Movie>();
        for (int j = 0; j < sizeArrMovieName; j++)
            //Crate movie object per movie includes the average score
            listMovies.add(new Movie(arrMovieName[j],
                    Math.round((hashScore.get(arrMovieName[j]) / hashCount.get(arrMovieName[j])) * 100000) / 100000d));
        listMovies.sort((movie1, movie2) -> {
            if (movie1.getScore() != movie2.getScore()) {
                return movie1.getScore() > movie2.getScore() ? -1 : 1;
            }
            else {
                return movie1.getProductId().compareTo(movie2.getProductId());
            }
        });

        //Checking if the value of 'topK' is greater than 'sizeArrMovieName' - containing the real value of movies size
        topK = (topK > sizeArrMovieName) ? sizeArrMovieName : topK;

        return listMovies.subList(0, (int)topK);
    }

    @Override
    public double totalMovieAverage(String productId) {
        double countReview = 0, scoreMovie = 0;

        // Over all reviews and sum the score of movie
        for(MovieReview item:this._movieReviews)
            if(item.getMovie().getProductId().compareTo(productId) == 0) {
                scoreMovie += item.getMovie().getScore();
                countReview++;
            }

        return (Math.round(scoreMovie / countReview * 100000d)/100000d);
    }

    @Override
    public Movie movieWithHighestAverage() {
        //getTopKMoviesAverage function return 'k' movies with highest score
        return getTopKMoviesAverage(1).get(0);
    }

    @Override
    public List<Movie> getMoviesPercentile(double percentile) {
        //Get list of all movie sort by average score
        List<Movie> listMovies = getTopKMoviesAverage(this._movieReviews.size());
        //Count of movies above percentile
        int countMoviesAbovePercentile = (int) (((100 - percentile) * listMovies.size()) / 100);
        //If countMoviesAbovePercentile is zero, should show at least one movie (according to output received)
        if(countMoviesAbovePercentile == 0)
            countMoviesAbovePercentile++;

        return listMovies.subList(0, countMoviesAbovePercentile);
    }

    @Override
    public String mostReviewedProduct() {
        // The function returns the movie with the most reviews
        Map<String, Long> map = reviewCountPerMovieTopKMovies(1);
        String str = "";
        //The for run only one time, and we get the name of movie
        for (Entry<String, Long> item : map.entrySet())
            str = item.getKey();

        return str;
    }

    @Override
    public Map<String, Long> reviewCountPerMovieTopKMovies(int topK) {
        Map<String, Long> movieIdToReviewsCount = new HashMap<>();
        Movie movie;
        int i = 0;
        String movieId;

        for (MovieReview movieReview : _movieReviews) {
            movie = movieReview.getMovie();
            movieId = movie.getProductId();
            if (movieIdToReviewsCount.containsKey(movie.getProductId())) {
                movieIdToReviewsCount.put(movieId, movieIdToReviewsCount.get(movieId) + 1);
            }
            else {
                movieIdToReviewsCount.put(movie.getProductId(),(long) 1);
            }
        }

        List<Entry<String, Long>> movieReviewsCountList =
                new LinkedList<>(movieIdToReviewsCount.entrySet());

        // Sorting the movies reviews count list based on number of reviews for each movie
        Collections.sort(movieReviewsCountList, (reviewsCount1, reviewsCount2)->
        {
            if (reviewsCount2.getValue().equals(reviewsCount1.getValue())){
                return reviewsCount2.getKey().compareTo(reviewsCount1.getKey());
            }
            else {
                return reviewsCount2.getValue().compareTo(reviewsCount1.getValue());
            }
        });

        Map<String, Long> sortedMovieIdToReviewsCount = new LinkedHashMap<>();
        for (Entry<String, Long> movieReviewsCount : movieReviewsCountList)
        {
            if (i == topK)
            {
                break;
            }
            sortedMovieIdToReviewsCount.put(movieReviewsCount.getKey(),
                    movieReviewsCount.getValue());
            i++;
        }
        return sortedMovieIdToReviewsCount;
    }

    @Override
    public String mostPopularMovieReviewedByKUsers(int numOfUsers) {

        List<Movie> listMovieScore = getTopKMoviesAverage(this._movieReviews.size());
        List<Map<String, String>> arrSumReviewPerMovie = new ArrayList<>();
        int sizeSizeMovies = listMovieScore.size();
        String[] arrNameMovie = new String[sizeSizeMovies];

        for (int j = sizeSizeMovies - 1; j >= 0; j--) {
            arrNameMovie[j] = listMovieScore.get(j).getProductId();
            arrSumReviewPerMovie.add(new HashMap<>());
        }
        boolean flagFoundMovie;
        for (MovieReview item : this._movieReviews) {
            flagFoundMovie = false;
            for (int j = sizeSizeMovies - 1; j >= 0 && !flagFoundMovie; j--) {
                if (arrNameMovie[j].compareTo(item.getMovie().getProductId()) == 0
                        && !arrSumReviewPerMovie.get(j).containsKey(item.getUserId())) {
                    arrSumReviewPerMovie.get(j).put(item.getUserId(), item.getMovie().getProductId());
                    flagFoundMovie = true;
                }
            }
        }
        for (int j = 0; j < sizeSizeMovies; j++) {
            if (arrSumReviewPerMovie.get(j).size() >= numOfUsers)
                return arrNameMovie[j];
        }
        return "Not found a movie!";
    }

    @Override
    public Map<String, Long> moviesReviewWordsCount(int topK) {
        return getWordCountForMovieReviews(_movieReviews, topK);
    }

    @Override
    public Map<String, Long> topYMoviewsReviewTopXWordsCount(int topMovies, int topWords) {
        Map<String, Long> movieToWordsCount = new HashMap<>();
        List<Movie> topMoviesList = getTopKMoviesAverage(topMovies);
        List<MovieReview> topMovieReviewsList = new ArrayList<>();
        for (MovieReview movieReview: _movieReviews){
            boolean movieExist = false;
            for (Movie movie : topMoviesList){
                if(movie.getProductId().equals(movieReview.getMovie().getProductId())){
                    movieExist = true;
                    break;
                }
            }
            if (movieExist){
                topMovieReviewsList.add(movieReview);
            }
        }
        return getWordCountForMovieReviews(topMovieReviewsList, topWords);
    }

    private Map<String, Long> getWordCountForMovieReviews
            (Collection<MovieReview> movies, int topK) {
        Map<String, Long> wordCountHashTable = new Hashtable<>();
        int i = 0;
        String[] wordsArray;
        for (MovieReview item : movies) {
            wordsArray = item.getReview().split(" ");
            for (String word : wordsArray) {
                if (wordCountHashTable.containsKey(word))
                    wordCountHashTable.put(word, wordCountHashTable.get(word) + 1);
                else
                    wordCountHashTable.put(word, (long) 1);
            }
        }
        List<Entry<String, Long>> wordCountList =
                new LinkedList<>(wordCountHashTable.entrySet());

        Collections.sort(wordCountList, (wordCount1, wordCount2) ->
        {
            if (wordCount2.getValue().equals(wordCount1.getValue())) {
                return wordCount1.getKey().compareTo(wordCount2.getKey());
            } else {
                return wordCount2.getValue().compareTo(wordCount1.getValue());
            }
        });

        Map<String, Long> sortedWordCount = new LinkedHashMap<>();
        for (Entry<String, Long> movieReviewsCount : wordCountList)
        {
            if (i == topK)
            {
                break;
            }
            sortedWordCount.put(movieReviewsCount.getKey(),
                    movieReviewsCount.getValue());
            i++;
        }
        return sortedWordCount;
    }

    @Override
    public Map<String, Double> topKHelpfullUsers(int k) {
        Map<String, Double> mapHelpfulUsers = new LinkedHashMap<>();
        Map<String, Double> mapCounterPerUser = new Hashtable<>();
        Map<String, Double> mapDenominatorPerUser = new Hashtable<>();

        if(k == 0)
            return  mapHelpfulUsers;

        Double counter, denominator;
        String[] str;
        for (MovieReview item : this._movieReviews) {
            str = item.getHelpfulness().split("/");
            counter = Double.parseDouble(str[0]);
            denominator = Double.parseDouble(str[1]);

            if (mapCounterPerUser.containsKey(item.getUserId())) {
                mapCounterPerUser.put(item.getUserId(), mapCounterPerUser.get(item.getUserId()) + counter);
                mapDenominatorPerUser.put(item.getUserId(), mapDenominatorPerUser.get(item.getUserId()) + denominator);
            }
            else {
                mapCounterPerUser.put(item.getUserId(), counter);
                mapDenominatorPerUser.put(item.getUserId(), denominator);
            }
        }
        for(Entry<String, Double>item:mapCounterPerUser.entrySet())
            if(!(mapCounterPerUser.get(item.getKey()) == 0 && mapDenominatorPerUser.get(item.getKey()) == 0)) {
                mapHelpfulUsers.put(item.getKey(), (Math.round((mapCounterPerUser.get(item.getKey())
                        / mapDenominatorPerUser.get(item.getKey())) *100000d) / 100000d));
            }

        List<Entry<String, Double>> listHelpfulUsers =
                new LinkedList<>(mapHelpfulUsers.entrySet());
        Collections.sort(listHelpfulUsers, new Comparator<Entry<String, Double>>() {
            @Override
            public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        Collections.sort(listHelpfulUsers, (wordCount1, wordCount2) ->
        {
            if (wordCount2.getValue().equals(wordCount1.getValue())) {
                return wordCount1.getKey().compareTo(wordCount2.getKey());
            } else {
                return wordCount2.getValue().compareTo(wordCount1.getValue());
            }
        });
        mapHelpfulUsers.clear();
        k = (k > listHelpfulUsers.size())?listHelpfulUsers.size():k;
        for(int i = 0; i < k; i++)
        {
            mapHelpfulUsers.put(listHelpfulUsers.get(i).getKey(), listHelpfulUsers.get(i).getValue());
        }

        return mapHelpfulUsers;

    }

    @Override
    public long moviesCount() {
        Map<String, Double> mapCouneMovies = new Hashtable<String, Double>();

        for (MovieReview item : this._movieReviews)
            mapCouneMovies.put(item.getMovie().getProductId(), 0.0);

        return mapCouneMovies.size();
    }
}