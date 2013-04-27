gamma = load('final.gamma');
gammanorm = zeros(size(gamma));
for i =1:size(gamma,1)
    sum = 0;
    sanitychecksum = 0;
    for j=1:size(gamma,2)
        sum = sum + gamma(i,j);
    end
    for j=1:size(gamma,2)
        alpha = gamma(i,j);
        
        % E(log_theta_j) = digamma(gamma_j) - digamma(sum_{gamma_i})
        
        % digamma approx
        % shi(alpha) = log(alpha) - 1/(2alpha)
        
        %not working
        %log_theta_j = log(alpha) - 1/(2*alpha) - ( log(sum) - %1/(2*sum));
        
        % log_theta_j = log(alpha)- ( log(sum) );
        % theta_j = exp(log_theta_j);
        
        %taking only the first term of the approximation
        theta_j = alpha / sum;
        gammanorm(i,j) = theta_j;
        sanitychecksum = sanitychecksum + gammanorm(i,j);
    end
    if(abs(sanitychecksum - 1) > 1e-2 )
        sanitychecksum
        error('check failed');
    end
    
end
csvwrite('topic.exact.prob',gammanorm);

%{
gamma = load('final.gamma');
gammanorm = zeros(size(gamma));
for i =1:size(gamma,1)
    sum = 0;
    for j=1:size(gamma,2)
        sum = sum + exp(gamma(i,j));
    end
    for j=1:size(gamma,2)
        gammanorm(i,j) = exp(gamma(i,j)) / sum;
    end
end
csvwrite('topic.prob',gammanorm)
%}