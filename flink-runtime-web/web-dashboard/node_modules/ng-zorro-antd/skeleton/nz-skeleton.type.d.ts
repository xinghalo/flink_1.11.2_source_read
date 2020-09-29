export declare type ParagraphWidth = number | string | Array<number | string>;
export declare type AvatarShape = 'square' | 'circle';
export declare type AvatarSize = 'small' | 'large' | 'default';
export interface NzSkeletonAvatar {
    size?: AvatarSize;
    shape?: AvatarShape;
}
export interface NzSkeletonTitle {
    width?: number | string;
}
export interface NzSkeletonParagraph {
    rows?: number;
    width?: ParagraphWidth;
}
