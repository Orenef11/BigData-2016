package univ.bigdata.course.movie;
/*
Nir Rosen; nirosen99@gmail.coml; 311450704
Ran Bakalo;ranbak@gmail.com; 305286197
Nadav Eichler; Nadaveichler@gmail.com; 308027325;
Zhai Yona; Zahiyona2006@gmail.com
Oren Efraimov;Orenef11@gmail.com; 305781544
*/


public class Movie {

    private String productId;

    private double score;

    public Movie() {
    }

    public Movie(String productId, double score) {
        this.productId = productId;
        this.score = score;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "productId='" + productId + '\'' +
                ", score=" + score +
                '}';
    }

}
