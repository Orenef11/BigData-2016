package univ.bigdata.course.providers;

import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.MoviesReviewsQueryRunner;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CollectionCertStoreParameters;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/*
Nir Rosen; nirosen99@gmail.coml; 311450704
Ran Bakalo;ranbak@gmail.com; 305286197
Nadav Eichler; Nadaveichler@gmail.com; 308027325;
Zhai Yona; Zahiyona2006@gmail.com
Oren Efraimov;Orenef11@gmail.com; 305781544
*/

import javax.swing.JOptionPane;

public class FileIOMoviesProvider implements MoviesProvider {

    private BufferedReader _bufferedReader;
    private String _nextLine;

    public FileIOMoviesProvider()
    {
        _nextLine = "";
        // todo: change to input dir
        FileReader fileReader;
        try
        {
            fileReader = new FileReader(MoviesReviewsQueryRunner.inputFile);
            _bufferedReader = new BufferedReader(fileReader);

        }
        catch (IOException ex)
        {
            //todo: do something with the exception (file not found)
            return;
        }

    }

    @Override
    public boolean hasMovie() {
        try {
        if (_nextLine != "" || (_nextLine = _bufferedReader.readLine()) != null){
            return true;
        }
        //If there are no more movies, Close the input stream
        _bufferedReader.close();
        }
        catch (IOException ex)
        {
            //todo: do something with the exception
            JOptionPane.showMessageDialog(null, "Problem with reading from file");
        }
        return  false;

    }

    @Override
    public MovieReview getMovie() {
        try {
            if (_nextLine != "" || (_nextLine = _bufferedReader.readLine()) != null) {
                String[] reviewFields = _nextLine.split("\t");
                _nextLine = "";
                return _extractFieldsToCollections(reviewFields);
            }
        }
        catch (IOException ex)
        {
            //todo: do something with the exception
        }
        return  null;
    }

    private MovieReview _extractFieldsToCollections(String[] reviewFields) {
        String productId="", userId="", profileName="", helpfulness = "", summary="", reviewText="";
        double score = 0;
        Date timestamp = new Date();
        for (String reviewField : reviewFields) {
            int indexOfSeperator = reviewField.indexOf(':');
            if (indexOfSeperator == -1) {
                break;
            }
            String reviewFieldKey = reviewField.substring(0, indexOfSeperator);
            String reviewFieldValue = reviewField.substring(indexOfSeperator + 2);

            switch (reviewFieldKey) {
                case "product/productId": {
                    productId = reviewFieldValue;
                    break;
                }
                case "review/userId": {
                    userId = reviewFieldValue;
                    break;
                }
                case "review/profileName": {
                    profileName = reviewFieldValue;
                    break;
                }
                case "review/helpfulness": {
                    helpfulness =  reviewFieldValue;
                    break;
                }
                case "review/score": {
                    score = Double.parseDouble(reviewFieldValue);
                    break;
                }
                case "review/time": {
                    timestamp = new Date(Long.parseLong(reviewFieldValue));
                    break;
                }
                case "review/summary": {
                    summary = reviewFieldValue;
                    break;
                }
                case "review/text": {
                    reviewText = reviewFieldValue;
                    break;
                }
            }
        }

        Movie movie = new Movie(productId, score);
        MovieReview review = new MovieReview(movie, userId,profileName, helpfulness, timestamp, summary, reviewText);
        return review;
    }
}
