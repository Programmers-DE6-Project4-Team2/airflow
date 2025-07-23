{{ config(
    materialized='incremental',
    unique_key='review_id',
    incremental_strategy='merge'
) }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY `no`
            ORDER BY scraped_at DESC
        ) AS row_num
    FROM {{ source('bronze', 'musinsa_reviews') }}
)

SELECT
    CAST(`no` AS STRING) AS review_id,
    CAST(product_id AS STRING) AS product_id,
    category_code,
    category_name,
    type,
    typeName,
    subType,
    content,
    commentCount,
    grade,
    goods_goodsNo,
    goods_goodsSubNo,
    goods_goodsName,
    goods_goodsImageFile,
    goods_goodsImageExtension,
    goods_goodsOptionKindCode,
    goods_brandName,
    goods_brandEnglishName,
    goods_brand,
    goods_brandBestYn,
    goods_brandConcatenation,
    goods_goodsCreateDate,
    goods_goodsImageIdx,
    goods_saleStatCode,
    goods_saleStatLabel,
    goods_goodsSex,
    goods_goodsSexClassification,
    goods_showSoldOut,
    userImageFile,
    goodsOption,
    commentReplyCount,
    userStaffYn,
    images,
    likeCount,
    userReactionType,
    createDate,
    goodsThumbnailImageUrl,
    userId,
    encryptedUserId AS user_id,
    userProfileInfo_userNickName,
    userProfileInfo_userLevel,
    userProfileInfo_userOutYn,
    userProfileInfo_userStaffYn,
    userProfileInfo_reviewSex,
    userProfileInfo_userWeight AS user_weight,
    userProfileInfo_userHeight AS user_height,
    userProfileInfo_userSkinInfo,
    userProfileInfo_skinType AS skin_type,
    userProfileInfo_skinTone AS skin_tone,
    userProfileInfo_skinWorry AS skin_worry,
    orderOptionNo,
    channelSource,
    channelSourceName,
    channelActivityId,      업로드하기
    relatedNo,
    isFirstReview AS is_first_review,
    reviewProfileTypeEnum AS profile_type,
    specialtyCodes,
    reviewerWeeklyRanking AS weekly_ranking,
    reviewerMonthlyRanking AS monthly_ranking,
    showUserProfile,
    scraped_at,
    '무신사' AS platform
FROM ranked
WHERE row_num = 1