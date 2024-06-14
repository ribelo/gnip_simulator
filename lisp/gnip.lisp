(defpackage gnip
  (:use :cl)
  (:import-from :alexandria)
  (:import-from :distributions))

(in-package gnip)

(defstruct user
  (id 0 :type (integer 0))
  (tweet-count 0 :type (integer 0)))

(defstruct tweet
  (user-id 0 :type alexandria:non-negative-fixnum)
  (timestamp 0 :type alexandria:non-negative-fixnum))

(declaim (ftype (function ((integer 0) (double-float 0d0)) (vector user))
                gen-users))
(defun gen-users (num-users shape)
  (let ((scale 10d0)
        (dist (distributions:r-log-normal 0d0 2.0d0))
        (users (make-array num-users :adjustable t :fill-pointer 0)))
    (dotimes (id num-users)
      (let ((tweet-count (max 0 (round (distributions:draw dist)))))
        (vector-push-extend (make-user :id id :tweet-count tweet-count) users)))
    users))

(declaim (ftype (function ((integer 0)
                           (double-float 0d0)
                           (integer 0)
                           (integer 0))
                          (values (vector user) hash-table))
                gen-test-data))
(defun gen-test-data (num-users shape start-time end-time)
  (let ((users (gen-users num-users shape))
        (user-tweets (make-hash-table :test 'eql)))
    (loop :for user :across users :do
      (dotimes (_ (user-tweet-count user))
        (let* ((offset (random (- end-time start-time)))
               (tweet-time (+ start-time offset))
               (tweet (make-tweet :user-id (user-id user)
                                  :timestamp tweet-time)))
          (vector-push-extend
            tweet
            (alexandria:ensure-gethash
              (user-id user)
              user-tweets
              (make-array 22 :adjustable t :fill-pointer 0))))))
    (values users user-tweets)))

(defstruct stats
  (num-requests 0 :type (integer 0))
  (num-users 0 :type (integer 0))
  (num-tweets 0 :type (integer 0))
  (avg-tweets-per-user 0 :type (integer 0))
  (avg-tweets-per-req 0 :type (integer 0)))

(declaim (ftype (function (stats (integer 0) hash-table) (values)) update-stats))
(defun update-stats (stats reqs data)
  (with-slots (num-requests num-users num-tweets avg-tweets-per-user avg-tweets-per-req) stats
    (incf num-requests reqs)
    (incf num-users (hash-table-count data))
    (incf num-tweets (loop :for v :being :the :hash-value :of data :sum v))
    (setf avg-tweets-per-user (truncate num-tweets num-users))
    (setf avg-tweets-per-req (truncate num-tweets num-requests))
    (values)))

(declaim (ftype (function (tweet tweet) boolean) tweet<))
(declaim (inline tweet<))
(defun tweet< (a b)
  (< (tweet-timestamp a) (tweet-timestamp b)))

(declaim (ftype (function ((vector user)
                           hash-table
                           (integer 0)
                           (integer 0)
                           (member :all :first))
                          (values (integer 0) hash-table))
                sim-request))
(defun sim-request (users users-tweets max-tweets/request min-tweets/user mode)
  (let ((results (make-hash-table))
        (tweets (make-array 0 :element-type 'tweet
                              :adjustable t
                              :fill-pointer 0)))
    (loop :for user :of-type user :across users :do
      (setf (gethash (user-id user) results) 0)
      (multiple-value-bind (user-tweets present-p)
          (gethash (user-id user) users-tweets)
        (when present-p
          (loop :for tweet :of-type tweet :across user-tweets :do
            (vector-push-extend tweet tweets)))))
    (locally (declare (inline sort)) (sort tweets #'tweet<))
    (let ((req-count 1)
          (idx 0)
          (batch-count 0))
      (loop :for tweet :across tweets :do
        (incf idx)
        (incf batch-count)
        (incf (gethash (tweet-user-id tweet) results))
        (when (zerop (mod batch-count max-tweets/request))
          (multiple-value-bind (max-user-tweets min-user-tweets)
              (loop :for v :being :the :hash-value :of results
                    :maximize v :into max
                    :minimize v :into min
                    :finally (return (values max min)))
            (case mode
              (:first
               (if (>= max-user-tweets min-tweets/user)
                   (return)
                   (incf req-count)))
              (:all
               (if (>= min-user-tweets min-tweets/user)
                   (return)
                   (incf req-count)))))))
      (values req-count results))))

(declaim (ftype (function ((vector user)
                           hash-table
                           (integer 0)
                           (integer 0)
                           (integer 0)
                           (member :all :first))
                          stats)
                sim-scenario))
(defun sim-scenario
    (users users-tweets max-tweets/request max-requests min-tweets/user mode)
  (let ((stats (make-stats)))
    (loop :for chunk :below (truncate (length users) 100)
          :for users-chunk
            := (make-array 100 :displaced-to users
                               :displaced-index-offset (* chunk 100))
          :do (multiple-value-bind (reqs data)
                  (sim-request users-chunk
                               users-tweets
                               max-tweets/request
                               min-tweets/user
                               mode)
                (update-stats stats reqs data)
                (when (>= (stats-num-requests stats) max-requests)
                  (return))))
    stats))

(defun run ()
  (let* ((num-users 10000000)
         (start-time (- (sb-ext:get-time-of-day) (* 7 24 60 60)))
         (end-time (sb-ext:get-time-of-day))
         (max-requests 12500))
    (multiple-value-bind (users tweets)
        (gen-test-data num-users 1.5d0 start-time end-time)
      (format t "total tweets: ~A~%"
              (loop :for v :of-type (vector tweet)
                      :being :the :hash-value :of tweets
                    :sum (length v)))
      ;; asc
      (format t "asc all~%")
      (sort users #'< :key #'user-tweet-count)
      (format t "~A~%" (sim-scenario users tweets 500 max-requests 100 :all))
      (format t "====~%")
      ;; desc
      (format t "desc all~%")
      (alexandria:nreversef users)
      (format t "~A~%" (sim-scenario users tweets 500 max-requests 100 :all))
      (format t "====~%")
      ;; random
      (format t "random all~%")
      (alexandria:shuffle users)
      (format t "~A~%" (sim-scenario users tweets 500 max-requests 100 :all))
      (format t "====~%")
      ;; asc first
      (format t "asc first~%")
      (sort users #'< :key #'user-tweet-count)
      (format t "~A~%" (sim-scenario users tweets 500 max-requests 100 :first))
      (format t "====~%")
      ;; desc first
      (format t "desc first~%")
      (alexandria:nreversef users)
      (format t "~A~%" (sim-scenario users tweets 500 max-requests 100 :first))
      (format t "====~%")
      ;; random first
      (format t "random first~%")
      (alexandria:shuffle users)
      (format t "~A~%" (sim-scenario users tweets 500 max-requests 100 :first))
      (format t "====~%"))))
