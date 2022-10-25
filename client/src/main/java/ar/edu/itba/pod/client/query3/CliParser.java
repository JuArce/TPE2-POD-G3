package ar.edu.itba.pod.client.query3;

import ar.edu.itba.pod.client.models.Arguments;
import ar.edu.itba.pod.client.utils.BaseParser;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Optional;

public class CliParser extends BaseParser {
    @Override
    protected Arguments createArgumentObject() {
        return new Arguments();
    }
    
    
    @Override
    protected void RegisterOptions(Options options){
        super.RegisterOptions(options);
        options.addRequiredOption("Dmin","Dmin",true,"minimum number of people");
    }
    
    @Override
    protected ar.edu.itba.pod.client.models.Arguments getArguments(CommandLine cmd){
        Arguments args = (Arguments) super.getArguments(cmd);
        
        try {
            args.setMin(Integer.parseInt(cmd.getOptionValue("Dmin")));
        }
        catch (NumberFormatException e) {
            logger.error("Invalid argument for Dmin: {}", cmd.getOptionValue("Dmin"));
        }
        return args;
    }
//    @Override
//    public Optional<Arguments> parse(String[] args){
//        Optional<Arguments> ret = (Optional<Arguments>) super.parse(args);
//        return super.parse(args);
//    }
    

    public static class Arguments extends ar.edu.itba.pod.client.models.Arguments{
        @Getter @Setter
        private int min = 0;
        
        @Override
        public boolean isValid() {
            return super.isValid() && min > 0;
        }
    }
}
